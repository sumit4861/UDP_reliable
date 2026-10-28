import java.io.FileOutputStream;
import java.net.InetAddress;
import java.util.Arrays;

/**
 * User2: Receiver application for KTP transfer.
 *
 * Listens for incoming data blocks over a KTP socket using k_recvfrom,
 * writes received in-order data blocks atomically to output.txt,
 * and verifies completion.
 */
public class User2 {
    public static final int BUFFER_SIZE = 1024;

    public static void main(String[] args) {
        String outputFile = (args.length >= 1) ? args[0] : "output.txt";
        int localPort = (args.length >= 2) ? Integer.parseInt(args[1]) : 8887;
        int remotePort = (args.length >= 3) ? Integer.parseInt(args[2]) : 9997;

        System.out.println("==================================================");
        System.out.println("           KTP Receiver (User2)                   ");
        System.out.println("==================================================");
        System.out.println("Output File: " + outputFile);
        System.out.println("Binding: local port " + localPort + " -> remote port " + remotePort);

        int fd = KtpSocket.k_socket();
        if (fd < 0) {
            System.err.println("Failed to create KTP socket (errno=" + KtpSocket.getErrno() + ")");
            return;
        }
        System.out.println("Created KTP Socket ID: " + fd);

        try {
            InetAddress localAddr = InetAddress.getByName("127.0.0.1");
            InetAddress remoteAddr = InetAddress.getByName("127.0.0.1");

            int bindRes = KtpSocket.k_bind(fd, localAddr, localPort, remoteAddr, remotePort);
            if (bindRes < 0) {
                System.err.println("Binding failed (errno=" + KtpSocket.getErrno() + ")");
                KtpSocket.k_close(fd);
                return;
            }
            System.out.println("Binding succeeded. Listening for incoming messages...");

            byte[] buffer = new byte[BUFFER_SIZE];
            byte[] eofMarker = "__KTP_EOF__".getBytes();

            long totalBytesReceived = 0;
            int totalMessagesReceived = 0;
            long emptyConsecutiveCount = 0;

            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                while (true) {
                    int n = KtpSocket.k_recvfrom(fd, buffer, buffer.length);

                    if (n < 0 && KtpSocket.getErrno() == KtpConstants.ENOMESSAGE) {
                        Thread.sleep(50);
                        emptyConsecutiveCount++;
                        // If we've already received data and no new message arrives for 10 seconds, finish
                        if (totalBytesReceived > 0 && emptyConsecutiveCount > 200) {
                            System.out.println("[User2] No more data received for timeout period. Finishing.");
                            break;
                        }
                        continue;
                    }

                    emptyConsecutiveCount = 0;

                    if (n < 0) {
                        System.err.println("Socket read error (errno=" + KtpSocket.getErrno() + ")");
                        break;
                    }

                    // Check for EOF sentinel
                    if (n == eofMarker.length) {
                        byte[] receivedChunk = Arrays.copyOf(buffer, n);
                        if (Arrays.equals(receivedChunk, eofMarker)) {
                            System.out.println("[User2] End-of-file sentinel received.");
                            break;
                        }
                    }

                    fos.write(buffer, 0, n);
                    fos.flush();
                    totalBytesReceived += n;
                    totalMessagesReceived++;

                    if (totalMessagesReceived % 50 == 0) {
                        System.out.printf("[User2] Received %d messages (%d bytes)...\n",
                                totalMessagesReceived, totalBytesReceived);
                    }
                }
            }

            System.out.println("\n----------------- Reception Summary -----------------");
            System.out.println("Output file: " + outputFile);
            System.out.println("Total messages written: " + totalMessagesReceived);
            System.out.println("Total bytes written: " + totalBytesReceived);
            System.out.println("-----------------------------------------------------\n");

            KtpSocket.k_close(fd);
            System.out.println("[User2] Socket closed. File reception completed.");

        } catch (Exception e) {
            e.printStackTrace();
            KtpSocket.k_close(fd);
        }
    }
}
