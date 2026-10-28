import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** End-to-end loopback transfer tests using the public KTP socket API. */
public final class ProtocolIntegrationTests {
    private static final byte[] EOF = new byte[] {
            (byte) 251, (byte) 252, (byte) 253, (byte) 254,
            (byte) 255, (byte) 251, (byte) 252, (byte) 253
    };

    public static void main(String[] args) throws Exception {
        runTransfer(0.0, 24080, 128 * 1024);
        runTransfer(0.10, 24082, 110 * 1024);
        System.out.println("PASS: large loopback transfer and simulated-loss transfer");
    }

    private static void runTransfer(double lossProbability, int receiverPort, int payloadSize) throws Exception {
        KtpSocket.setProtocolParams(1, lossProbability);
        byte[] expected = new byte[payloadSize];
        for (int i = 0; i < expected.length; i++) expected[i] = (byte) (i % 251);

        CountDownLatch receiverReady = new CountDownLatch(1);
        AtomicReference<Throwable> receiverFailure = new AtomicReference<>();
        ByteArrayOutputStream received = new ByteArrayOutputStream();

        Thread receiver = new Thread(() -> {
            int fd = -1;
            try {
                fd = KtpSocket.k_socket();
                if (fd < 0) throw new AssertionError("receiver k_socket failed: " + KtpSocket.getErrno());
                int bind = KtpSocket.k_bind(fd, InetAddress.getByName("127.0.0.1"), receiverPort,
                        InetAddress.getByName("127.0.0.1"), receiverPort + 1);
                if (bind < 0) throw new AssertionError("receiver bind failed: " + KtpSocket.getErrno());
                receiverReady.countDown();

                byte[] buffer = new byte[KtpConstants.MAX_MESSAGE_SIZE];
                long deadline = System.currentTimeMillis() + 90000;
                while (System.currentTimeMillis() < deadline) {
                    int n = KtpSocket.k_recvfrom(fd, buffer, buffer.length);
                    if (n < 0) {
                        if (KtpSocket.getErrno() == KtpConstants.ENOMESSAGE) {
                            Thread.sleep(2);
                            continue;
                        }
                        throw new AssertionError("receiver read failed: " + KtpSocket.getErrno());
                    }
                    if (n == EOF.length && Arrays.equals(Arrays.copyOf(buffer, n), EOF)) {
                        // Keep the receive socket alive for duplicate EOF retransmissions
                        // if the final ACK was lost.
                        Thread.sleep(KtpConstants.T * 3000L);
                        return;
                    }
                    received.write(buffer, 0, n);
                }
                throw new AssertionError("receiver timed out waiting for EOF");
            } catch (Throwable t) {
                receiverFailure.set(t);
                receiverReady.countDown();
            } finally {
                if (fd >= 0) KtpSocket.k_close(fd);
            }
        }, "KTP-integration-receiver");
        receiver.start();

        if (!receiverReady.await(5, TimeUnit.SECONDS)) throw new AssertionError("receiver failed to bind");
        if (receiverFailure.get() != null) throw new AssertionError("receiver startup failed", receiverFailure.get());

        int senderFd = KtpSocket.k_socket();
        if (senderFd < 0) throw new AssertionError("sender k_socket failed: " + KtpSocket.getErrno());
        try {
            int bind = KtpSocket.k_bind(senderFd, InetAddress.getByName("127.0.0.1"), receiverPort + 1,
                    InetAddress.getByName("127.0.0.1"), receiverPort);
            if (bind < 0) throw new AssertionError("sender bind failed: " + KtpSocket.getErrno());

            for (int offset = 0; offset < expected.length;) {
                int len = Math.min(KtpConstants.MAX_MESSAGE_SIZE, expected.length - offset);
                byte[] chunk = Arrays.copyOfRange(expected, offset, offset + len);
                enqueueWithBackpressure(senderFd, chunk);
                offset += len;
            }
            enqueueWithBackpressure(senderFd, EOF);

            long deadline = System.currentTimeMillis() + 90000;
            while (!KtpSocket.isAllAcknowledged(senderFd) && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            if (!KtpSocket.isAllAcknowledged(senderFd)) {
                throw new AssertionError("sender timed out waiting for ACKs at loss probability " + lossProbability);
            }

            receiver.join(5000);
            if (receiver.isAlive()) throw new AssertionError("receiver did not finish after EOF");
            if (receiverFailure.get() != null) throw new AssertionError("receiver failed", receiverFailure.get());
            check(Arrays.equals(expected, received.toByteArray()),
                    "byte-for-byte integrity failed at loss probability " + lossProbability);
            System.out.println("PASS: bytes=" + payloadSize + ", p=" + lossProbability);
        } finally {
            KtpSocket.k_close(senderFd);
        }
    }

    private static void enqueueWithBackpressure(int fd, byte[] message) throws Exception {
        long deadline = System.currentTimeMillis() + 30000;
        while (true) {
            int n = KtpSocket.k_sendto(fd, message, message.length);
            if (n == message.length) return;
            if (KtpSocket.getErrno() != KtpConstants.ENOSPACE) {
                throw new AssertionError("send failed: errno=" + KtpSocket.getErrno());
            }
            if (System.currentTimeMillis() >= deadline) throw new AssertionError("sender buffer stayed full");
            Thread.sleep(2);
        }
    }

    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
}
