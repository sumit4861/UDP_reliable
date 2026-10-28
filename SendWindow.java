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

    private int nextSeq = 1;
    private int baseSeq = 1;
    private int advertisedRwnd = KtpConstants.WINDOW_SIZE;
    private long lastZeroProbeTime = 0;

    /** Enqueues one complete KTP message, rejecting invalid sizes and full buffers. */
    public synchronized boolean enqueue(byte[] data, int length) {
        if (data == null || length <= 0 || length > KtpConstants.MAX_MESSAGE_SIZE || length > data.length) {
            return false;
        }
        if (sendBuffer.size() + inFlightList.size() >= maxBufferSize) {
            return false;
        }

        KtpPacket pkt = KtpPacket.createDataPacket(nextSeq, data, length);
        nextSeq = (nextSeq + 1) % KtpConstants.MAX_SEQ_NUM;
        sendBuffer.addLast(pkt);
        return true;
    }

    /** Moves queued messages into the advertised sending window. */
    public synchronized List<KtpPacket> getPacketsToSend() {
        List<KtpPacket> ready = new ArrayList<>();
        int effectiveWin = Math.min(KtpConstants.WINDOW_SIZE, advertisedRwnd);
        while (inFlightList.size() < effectiveWin && !sendBuffer.isEmpty()) {
            KtpPacket pkt = sendBuffer.removeFirst();
            pkt.timestamp = System.currentTimeMillis();
            pkt.lastSentTime = pkt.timestamp;
            if (pkt.firstSentTime == 0) pkt.firstSentTime = pkt.timestamp;
            pkt.transmitCount++;
            inFlightList.addLast(pkt);
            ready.add(pkt);
        }
        return ready;
    }

    /** Retransmits the entire outstanding window when its oldest packet times out. */
    public synchronized List<KtpPacket> checkTimeout(long timeoutMillis) {
        List<KtpPacket> retxList = new ArrayList<>();
        if (inFlightList.isEmpty()) return retxList;

        long now = System.currentTimeMillis();
        KtpPacket oldest = inFlightList.peekFirst();
        if (oldest != null && now - oldest.lastSentTime >= timeoutMillis) {
            for (KtpPacket pkt : inFlightList) {
                pkt.type = KtpConstants.TYPE_RETRANSMIT;
                pkt.lastSentTime = now;
                pkt.transmitCount++;
                retxList.add(pkt);
            }
        }
        return retxList;
    }

    /** Emits a persistence probe while the receiver advertises a zero window. */
    public synchronized KtpPacket checkZeroWindowProbe(long probeIntervalMillis) {
        long now = System.currentTimeMillis();
        if (advertisedRwnd == 0 && (!sendBuffer.isEmpty() || !inFlightList.isEmpty())
                && now - lastZeroProbeTime >= probeIntervalMillis) {
            lastZeroProbeTime = now;
            return KtpPacket.createWindowProbe(baseSeq);
        }
        return null;
    }

    /**
     * Processes cumulative ACKs using an unambiguous live-window distance.
     * Stale ACKs are ignored entirely; a duplicate ACK for the last acknowledged
     * sequence may still update the receiver's advertised free space.
     */
    public synchronized int processAck(int ackNum, int newRwnd) {
        if (ackNum < 0 || ackNum >= KtpConstants.MAX_SEQ_NUM) return 0;
        int safeRwnd = Math.max(0, Math.min(newRwnd, KtpConstants.WINDOW_SIZE));
        int lastAck = (baseSeq - 1 + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;

        if (inFlightList.isEmpty()) {
            if (ackNum == lastAck) advertisedRwnd = safeRwnd;
            return 0;
        }

        int oldestSeq = inFlightList.peekFirst().seqNum;
        int newestSeq = inFlightList.peekLast().seqNum;
        int inFlightSpan = (newestSeq - oldestSeq + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;
        int ackDist = (ackNum - oldestSeq + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;

        if (ackDist > inFlightSpan) {
            if (ackNum == lastAck) advertisedRwnd = safeRwnd;
            return 0;
        }

        advertisedRwnd = safeRwnd;
        int ackedCount = 0;
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
        return ackedCount;
    }

    public synchronized boolean isAllAcknowledged() {
        return sendBuffer.isEmpty() && inFlightList.isEmpty();
    }

    public synchronized int getInFlightCount() { return inFlightList.size(); }
    public synchronized int getSendBufferCount() { return sendBuffer.size(); }
    public synchronized int getAdvertisedRwnd() { return advertisedRwnd; }
    public synchronized int getBaseSeq() { return baseSeq; }
    public synchronized int getNextSeq() { return nextSeq; }
}
