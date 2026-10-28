import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * SendWindow maintains the sender-side message buffer, sliding window,
 * sequence numbers, unacknowledged packets, and retransmission timeout tracking.
 */
public class SendWindow {
    private final int maxBufferSize = KtpConstants.WINDOW_SIZE;
    private final LinkedList<KtpPacket> sendBuffer = new LinkedList<>();
    private final LinkedList<KtpPacket> inFlightList = new LinkedList<>();

    private int nextSeq = 1;       // Sequence numbers start at 1
    private int baseSeq = 1;       // Base of sender window
    private int advertisedRwnd = KtpConstants.WINDOW_SIZE; // Initially 10
    private long lastZeroProbeTime = 0;

    /**
     * Enqueues a new message block from the application (k_sendto).
     * Returns true if successfully queued, false if send buffer is full (ENOSPACE).
     */
    public synchronized boolean enqueue(byte[] data, int length) {
        int totalBuffered = sendBuffer.size() + inFlightList.size();
        if (totalBuffered >= maxBufferSize) {
            return false;
        }

        KtpPacket pkt = KtpPacket.createDataPacket(nextSeq, data, length);
        nextSeq = (nextSeq + 1) % KtpConstants.MAX_SEQ_NUM;

        sendBuffer.addLast(pkt);
        return true;
    }

    /**
     * Retrieves packets ready to be transmitted over UDP within the current window limit.
     * Window limit = min(WINDOW_SIZE, advertisedRwnd).
     */
    public synchronized List<KtpPacket> getPacketsToSend() {
        List<KtpPacket> ready = new ArrayList<>();
        int effectiveWin = Math.min(KtpConstants.WINDOW_SIZE, advertisedRwnd);

        while (inFlightList.size() < effectiveWin && !sendBuffer.isEmpty()) {
            KtpPacket pkt = sendBuffer.removeFirst();
            pkt.timestamp = System.currentTimeMillis();
            pkt.lastSentTime = pkt.timestamp;
            if (pkt.firstSentTime == 0) {
                pkt.firstSentTime = pkt.timestamp;
            }
            pkt.transmitCount++;
            inFlightList.addLast(pkt);
            ready.add(pkt);
        }
        return ready;
    }

    /**
     * Checks if unacknowledged packets have timed out (> T seconds).
     * If timed out, returns all in-flight packets in the current window for retransmission.
     */
    public synchronized List<KtpPacket> checkTimeout(long timeoutMillis) {
        List<KtpPacket> retxList = new ArrayList<>();
        if (inFlightList.isEmpty()) {
            return retxList;
        }

        long now = System.currentTimeMillis();
        KtpPacket oldest = inFlightList.peekFirst();
        if (oldest != null && (now - oldest.lastSentTime >= timeoutMillis)) {
            // Retransmit all unacknowledged messages currently in swnd (Go-Back-N)
            for (KtpPacket pkt : inFlightList) {
                pkt.type = KtpConstants.TYPE_RETRANSMIT;
                pkt.lastSentTime = now;
                pkt.transmitCount++;
                retxList.add(pkt);
            }
        }
        return retxList;
    }

    /**
     * Checks if a zero-window probe should be sent when advertisedRwnd == 0.
     */
    public synchronized KtpPacket checkZeroWindowProbe(long probeIntervalMillis) {
        long now = System.currentTimeMillis();
        if (advertisedRwnd == 0 && (!sendBuffer.isEmpty() || !inFlightList.isEmpty())) {
            if (now - lastZeroProbeTime >= probeIntervalMillis) {
                lastZeroProbeTime = now;
                return KtpPacket.createWindowProbe(baseSeq);
            }
        }
        return null;
    }

    /**
     * Processes an incoming ACK packet.
     * Slides window up to ackNum, removes acknowledged packets from inFlightList,
     * and updates advertisedRwnd.
     */
    public synchronized int processAck(int ackNum, int newRwnd) {
        this.advertisedRwnd = Math.max(0, Math.min(newRwnd, KtpConstants.WINDOW_SIZE));
        int ackedCount = 0;

        if (inFlightList.isEmpty()) {
            return 0;
        }

        int oldestSeq = inFlightList.peekFirst().seqNum;
        int newestSeq = inFlightList.peekLast().seqNum;
        int inFlightSpan = (newestSeq - oldestSeq + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;
        int ackDist = (ackNum - oldestSeq + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;

        // Check if ackNum acknowledges any packet in flight (between oldestSeq and newestSeq)
        if (ackDist <= inFlightSpan) {
            while (!inFlightList.isEmpty()) {
                KtpPacket head = inFlightList.peekFirst();
                int headDist = (head.seqNum - oldestSeq + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;
                if (headDist <= ackDist) {
                    inFlightList.removeFirst();
                    ackedCount++;
                    baseSeq = (head.seqNum + 1) % KtpConstants.MAX_SEQ_NUM;
                } else {
                    break;
                }
            }
        }
        return ackedCount;
    }

    public synchronized boolean isAllAcknowledged() {
        return sendBuffer.isEmpty() && inFlightList.isEmpty();
    }

    public synchronized int getInFlightCount() {
        return inFlightList.size();
    }

    public synchronized int getSendBufferCount() {
        return sendBuffer.size();
    }

    public synchronized int getAdvertisedRwnd() {
        return advertisedRwnd;
    }

    public synchronized int getBaseSeq() {
        return baseSeq;
    }

    public synchronized int getNextSeq() {
        return nextSeq;
    }
}
