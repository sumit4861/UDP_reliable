public class KtpConstants {
    // Protocol parameters specified in assignment
    public static final int MAX_KTP_SOCKETS = 10;
    public static final int MAX_MESSAGE_SIZE = 512;
    public static final int WINDOW_SIZE = 10;
    public static final int MAX_SEQ_NUM = 256; // 8-bit sequence number (1..255 modulo 256)

    // Configurable protocol timing and loss parameters
    // T = 5 seconds (as specified in PDF)
    public static volatile int T = 5;
    // p = packet drop probability (0.0 to 1.0)
    public static volatile double P = 0.2;

    // Socket type
    public static final int SOCK_KTP = 3;

    // Error codes
    public static final int ENOTBOUND = 1001;
    public static final int ENOSPACE = 1002;
    public static final int ENOMESSAGE = 1003;
    public static final int ERRORNP = 1004;
    public static final int EBIND = 1005;
    public static final int ETOOSMALL = 1006;

    // Packet types
    public static final byte TYPE_DATA = 0;
    public static final byte TYPE_RETRANSMIT = 1;
    public static final byte TYPE_ACK = 2;
    public static final byte TYPE_WINDOW_PROBE = 3;
    public static final byte TYPE_FIN = 4;
    public static final byte TYPE_FIN_ACK = 5;

    // IPC Port for daemon (representing shared memory IPC)
    public static final int DEFAULT_IPC_PORT = 10039;

    /**
     * dropMessage function as required by assignment specification:
     * Generates a random number between 0 and 1. If < p, returns true (1), else false (0).
     */
    public static boolean dropMessage(double p) {
        if (p <= 0.0) return false;
        if (p >= 1.0) return true;
        return Math.random() < p;
    }
}
