import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.Arrays;

public class TestTransfer {
    public static void main(String[] args) throws Exception {
        double p = 0.2; // 20% drop probability
        KtpSocket.setProtocolParams(1, p); // 1s timeout for testing

        File input = new File("input.txt");
        File output = new File("test_output.txt");
        if (output.exists()) output.delete();

        System.out.println("Starting transfer test with p=" + p + " and T=2s...");

        Thread receiver = new Thread(() -> {
            try {
                int rfd = KtpSocket.k_socket();
                KtpSocket.k_bind(rfd, InetAddress.getByName("127.0.0.1"), 8887,
                        InetAddress.getByName("127.0.0.1"), 9997);
                System.out.println("Receiver bound on 8887.");

                byte[] buf = new byte[KtpConstants.MAX_MESSAGE_SIZE * 2];
                byte[] eof = "__KTP_EOF__".getBytes();

                try (FileOutputStream fos = new FileOutputStream(output)) {
                    while (true) {
                        int n = KtpSocket.k_recvfrom(rfd, buf, buf.length);
                        if (n < 0 && KtpSocket.getErrno() == KtpConstants.ENOMESSAGE) {
                            Thread.sleep(10);
                            continue;
                        }
                        if (n < 0) {
                            System.err.println("Receiver got error errno=" + KtpSocket.getErrno());
                            break;
                        }
                        if (n == eof.length && Arrays.equals(Arrays.copyOf(buf, n), eof)) {
                            System.out.println("Receiver got EOF sentinel!");
                            break;
                        }
                        fos.write(buf, 0, n);
                    }
                }
                Thread.sleep(KtpConstants.T * 3000L);
                KtpSocket.k_close(rfd);
                System.out.println("Receiver closed socket.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        receiver.start();
        Thread.sleep(200);

        int sfd = KtpSocket.k_socket();
        KtpSocket.k_bind(sfd, InetAddress.getByName("127.0.0.1"), 9997,
                InetAddress.getByName("127.0.0.1"), 8887);
        System.out.println("Sender bound on 9997.");

        byte[] sbuf = new byte[KtpConstants.MAX_MESSAGE_SIZE];
        int msgCount = 0;

        try (FileInputStream fis = new FileInputStream(input)) {
            int n;
            while ((n = fis.read(sbuf)) > 0) {
                int res = KtpSocket.k_sendto(sfd, sbuf, n);
                while (res < 0 && KtpSocket.getErrno() == KtpConstants.ENOSPACE) {
                    Thread.sleep(10);
                    res = KtpSocket.k_sendto(sfd, sbuf, n);
                }
                msgCount++;
            }
        }

        byte[] eof = "__KTP_EOF__".getBytes();
        while (KtpSocket.k_sendto(sfd, eof, eof.length) < 0) {
            Thread.sleep(10);
        }
        msgCount++;
        System.out.println("Sender finished enqueuing " + msgCount + " messages.");

        receiver.join(30000);

        int[] stats = KtpSocket.avg_calc(sfd);
        KtpSocket.k_close(sfd);

        System.out.println("Sender closed. Total transmissions: " + stats[0] + ", Messages: " + msgCount);
        System.out.println("Input length: " + input.length() + ", Output length: " + output.length());

        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] inHash = md.digest(new FileInputStream(input).readAllBytes());
        md.reset();
        byte[] outHash = md.digest(new FileInputStream(output).readAllBytes());

        boolean match = Arrays.equals(inHash, outHash);
        System.out.println("MD5 Match: " + match + (match ? " SUCCESS!" : " MISMATCH!"));
        if (match) {
            System.out.printf("Average transmissions per message: %.2f\n", (double)stats[0] / msgCount);
        }
    }
}
