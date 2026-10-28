import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * EvaluateP: Automated test harness to evaluate KTP protocol performance.
 *
 * Runs tests varying drop probability p from 0.05 to 0.50 in steps of 0.05.
 * Measures messages generated, total transmissions, and calculates average
 * transmissions per message (transmissions / messages).
 * Verifies 100% data integrity with MD5 checksum verification.
 */
public class EvaluateP {

    public static String computeMd5(File file) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = fis.read(buf)) > 0) {
                md.update(buf, 0, n);
            }
        }
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        String testFilename = (args.length > 0) ? args[0] : "input.txt";
        File inputFile = new File(testFilename);

        if (!inputFile.exists()) {
            // Check parent or sibling directory
            File alt = new File("../KGP_Transfer_Protocol/" + testFilename);
            if (alt.exists()) {
                inputFile = alt;
            } else {
                System.err.println("Input file not found: " + testFilename);
                return;
            }
        }

        String originalMd5 = computeMd5(inputFile);
        int timeoutT = (args.length > 1) ? Integer.parseInt(args[1]) : 2;
        KtpConstants.T = timeoutT;
        System.out.println("================================================================");
        System.out.println("  KTP Protocol Evaluation: Varying Drop Probability p           ");
        System.out.println("================================================================");
        System.out.println("Input file: " + inputFile.getAbsolutePath() + " (" + inputFile.length() + " bytes)");
        System.out.println("Input MD5:  " + originalMd5);
        System.out.println("Timeout T:  " + KtpConstants.T + " seconds");
        System.out.println("Window size: " + KtpConstants.WINDOW_SIZE + " messages");
        System.out.println("----------------------------------------------------------------");
        System.out.printf("%-8s | %-12s | %-15s | %-10s | %-10s\n",
                "Prob (p)", "Messages (M)", "Transmissions", "Ratio (T/M)", "Integrity");
        System.out.println("----------------------------------------------------------------");

        double[] pValues = {0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50};

        for (int idx = 0; idx < pValues.length; idx++) {
            double p = pValues[idx];
            int portR = 8800 + (idx * 2);
            int portS = portR + 1;
            File tempOutput = new File("output_eval_" + String.format("%.2f", p) + ".txt");

            // Set protocol drop probability
            KtpSocket.setProtocolParams(KtpConstants.T, p);

            // Start Receiver thread
            Thread receiverThread = new Thread(() -> {
                try {
                    int rfd = KtpSocket.k_socket();
                    KtpSocket.k_bind(rfd, InetAddress.getByName("127.0.0.1"), portR,
                            InetAddress.getByName("127.0.0.1"), portS);

                    byte[] buffer = new byte[KtpConstants.MAX_MESSAGE_SIZE * 2];
                    byte[] eofMarker = "__KTP_EOF__".getBytes();

                    try (FileOutputStream fos = new FileOutputStream(tempOutput)) {
                        while (true) {
                            int n = KtpSocket.k_recvfrom(rfd, buffer, buffer.length);
                            if (n < 0 && KtpSocket.getErrno() == KtpConstants.ENOMESSAGE) {
                                Thread.sleep(20);
                                continue;
                            }
                            if (n < 0) break;
                            if (n == eofMarker.length) {
                                byte[] slice = Arrays.copyOf(buffer, n);
                                if (Arrays.equals(slice, eofMarker)) {
                                    break;
                                }
                            }
                            fos.write(buffer, 0, n);
                        }
                    }
                    Thread.sleep(500);
                    KtpSocket.k_close(rfd);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            receiverThread.start();
            Thread.sleep(300);

            // Sender execution
            int sfd = KtpSocket.k_socket();
            KtpSocket.k_bind(sfd, InetAddress.getByName("127.0.0.1"), portS,
                    InetAddress.getByName("127.0.0.1"), portR);

            byte[] sbuf = new byte[KtpConstants.MAX_MESSAGE_SIZE];
            int msgCount = 0;

            try (FileInputStream fis = new FileInputStream(inputFile)) {
                int n;
                while ((n = fis.read(sbuf)) > 0) {
                    int res = KtpSocket.k_sendto(sfd, sbuf, n);
                    while (res < 0 && KtpSocket.getErrno() == KtpConstants.ENOSPACE) {
                        Thread.sleep(20);
                        res = KtpSocket.k_sendto(sfd, sbuf, n);
                    }
                    msgCount++;
                }
            }

            // Send EOF
            byte[] eofMarker = "__KTP_EOF__".getBytes();
            while (KtpSocket.k_sendto(sfd, eofMarker, eofMarker.length) < 0) {
                Thread.sleep(20);
            }
            msgCount++;

            // Wait for all sent packets to be acknowledged
            long waitStart = System.currentTimeMillis();
            while (!KtpSocket.isAllAcknowledged(sfd) && (System.currentTimeMillis() - waitStart < 30000)) {
                Thread.sleep(50);
            }

            // Wait for receiver completion
            receiverThread.join(30000);

            int[] stats = KtpSocket.avg_calc(sfd);
            int totalTrans = stats[0];
            KtpSocket.k_close(sfd);

            // Verify integrity
            String receivedMd5 = tempOutput.exists() ? computeMd5(tempOutput) : "N/A";
            boolean passed = originalMd5.equals(receivedMd5);
            double ratio = (msgCount > 0) ? ((double) totalTrans / msgCount) : 0.0;

            System.out.printf("%-8.2f | %-12d | %-15d | %-10.2f | %-10s\n",
                    p, msgCount, totalTrans, ratio, passed ? "PASS" : "FAIL");

            tempOutput.deleteOnExit();
            Thread.sleep(200);
        }

        System.out.println("----------------------------------------------------------------");
        System.out.println("Evaluation complete. All results verified with MD5 integrity.");
    }
}
