import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;

/**
 * User1: Sender application for KTP transfer.
 *
 * Reads an input file in 512-byte blocks, sends it over a KTP socket using k_sendto,
 * handles buffer full conditions (ENOSPACE), and prints transmission statistics
 * including average transmissions per message.
 */
public class User1 {
    public static final int BUFFER_SIZE = KtpConstants.MAX_MESSAGE_SIZE; // 512 bytes

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java User1 <filename> [localPort] [remotePort] [p]");
            System.out.println("Default: localPort=9997, remotePort=8887, p=" + KtpConstants.P);
            return;
        }

        String filename = args[0];
        int localPort = (args.length >= 2) ? Integer.parseInt(args[1]) : 9997;
        int remotePort = (args.length >= 3) ? Integer.parseInt(args[2]) : 8887;

        if (args.length >= 4) {
            double p = Double.parseDouble(args[3]);
            KtpSocket.setProtocolParams(KtpConstants.T, p);
            System.out.println("[User1] Set drop probability p = " + p);
        }

        File file = new File(filename);
        if (!file.exists() || !file.isFile()) {
            System.err.println("Error: File not found: " + filename);
            return;
        }

        System.out.println("==================================================");
        System.out.println("           KTP Sender (User1)                     ");
        System.out.println("==================================================");
        System.out.println("File: " + filename + " (" + file.length() + " bytes)");
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
            System.out.println("Binding succeeded.");

            // Allow receiver a moment to initialize
            Thread.sleep(1000);

            byte[] buffer = new byte[BUFFER_SIZE];
            int noOfMsgs = 0;

            long startTime = System.currentTimeMillis();

            try (FileInputStream fis = new FileInputStream(file)) {
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) > 0) {
                    int status = KtpSocket.k_sendto(fd, buffer, bytesRead);

                    while (status < 0 && KtpSocket.getErrno() == KtpConstants.ENOSPACE) {
                        // Send buffer full, backoff and retry
                        Thread.sleep(50);
                        status = KtpSocket.k_sendto(fd, buffer, bytesRead);
                    }

                    if (status < 0) {
                        System.err.println("Socket transmission error (errno=" + KtpSocket.getErrno() + ")");
                        break;
                    }

                    noOfMsgs++;
                    if (noOfMsgs % 50 == 0) {
                        System.out.printf("[User1] Queued %d messages (%d bytes)...\n", noOfMsgs, noOfMsgs * BUFFER_SIZE);
                    }
                }
            }

            // Send end-of-transmission sentinel (empty payload or marker)
            byte[] eofMarker = "__KTP_EOF__".getBytes();
            while (KtpSocket.k_sendto(fd, eofMarker, eofMarker.length) < 0) {
                Thread.sleep(50);
            }
            noOfMsgs++;

            System.out.println("[User1] All data blocks queued (" + noOfMsgs + " messages). Waiting for acknowledgements...");

            // Wait for transmission and ACKs to settle
            Thread.sleep(2000);

            long duration = System.currentTimeMillis() - startTime;
            int[] stats = KtpSocket.avg_calc(fd);
            int totalTrans = stats[0];

            System.out.println("\n----------------- Transfer Summary -----------------");
            System.out.println("File: " + filename + " (" + file.length() + " bytes)");
            System.out.println("Total messages sent: " + noOfMsgs);
            System.out.println("Total transmissions (incl. retransmissions): " + totalTrans);
            double ratio = (noOfMsgs > 0) ? ((double) totalTrans / noOfMsgs) : 0.0;
            System.out.printf("Average transmissions per message: %.4f\n", ratio);
            System.out.printf("Elapsed time: %.2f seconds\n", duration / 1000.0);
            System.out.println("----------------------------------------------------\n");

            // Allow time for receiver to process and ACK
            Thread.sleep(2000);
            KtpSocket.k_close(fd);
            System.out.println("[User1] Socket closed successfully.");

        } catch (Exception e) {
            e.printStackTrace();
            KtpSocket.k_close(fd);
        }
    }
}
