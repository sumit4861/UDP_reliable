import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Receiver-side KTP window. Buffers in-order messages for the application and
 * temporarily holds out-of-order messages until the missing sequence arrives.
 */
public class ReceiveWindow {
    private final int windowCapacity = KtpConstants.WINDOW_SIZE;
    private final LinkedList<byte[]> inOrderQueue = new LinkedList<>();
    private final Map<Integer, byte[]> outOfOrderMap = new HashMap<>();

    private int expSeq = 1;
    private int lastAckedSeq = 0;
    private boolean hasAck = false;
    private boolean nospace = false;
    private long lastRecoveryAckTime = 0;

    public synchronized int getFreeSpace() {
        return Math.max(0, windowCapacity - inOrderQueue.size() - outOfOrderMap.size());
    }

    /**
     * Processes an accepted DATA/RETRANSMIT packet. Out-of-order packets are
     * buffered but deliberately do not trigger an ACK, matching the assignment.
     */
    public synchronized KtpPacket processDataPacket(KtpPacket pkt) {
        if (pkt == null || pkt.seqNum < 0 || pkt.seqNum >= KtpConstants.MAX_SEQ_NUM
                || pkt.len < 0 || pkt.len > KtpConstants.MAX_MESSAGE_SIZE
                || pkt.payload == null || pkt.payload.length < pkt.len) {
            return hasAck ? KtpPacket.createAckPacket(lastAckedSeq, getFreeSpace()) : null;
        }

        int seq = pkt.seqNum;
        int dist = (seq - expSeq + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;
        int free = getFreeSpace();
        int occupiedOutOfOrder = outOfOrderMap.size();
        // A sliding receive window covers buffered out-of-order slots as well
        // as currently free slots, capped at the configured window size.
        int receiveSpan = Math.min(windowCapacity, free + occupiedOutOfOrder);

        // Anything at least half a sequence space ahead is old/stale under
        // serial-number arithmetic; the live window is at most 10 packets.
        if (dist >= KtpConstants.MAX_SEQ_NUM / 2 || outOfOrderMap.containsKey(seq)) {
            return hasAck ? KtpPacket.createAckPacket(lastAckedSeq, free) : null;
        }

        if (dist >= receiveSpan || free == 0) {
            return hasAck ? KtpPacket.createAckPacket(lastAckedSeq, free) : null;
        }

        byte[] data = new byte[pkt.len];
        System.arraycopy(pkt.payload, 0, data, 0, pkt.len);

        if (dist == 0) {
            inOrderQueue.addLast(data);
            advanceExpSeq();
            while (outOfOrderMap.containsKey(expSeq)) {
                inOrderQueue.addLast(outOfOrderMap.remove(expSeq));
                advanceExpSeq();
            }

            lastAckedSeq = (expSeq - 1 + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;
            hasAck = true;
            int updatedRwnd = getFreeSpace();
            nospace = updatedRwnd == 0;
            lastRecoveryAckTime = System.currentTimeMillis();
            return KtpPacket.createAckPacket(lastAckedSeq, updatedRwnd);
        }

        // Out-of-order: store only once, and intentionally do not ACK.
        outOfOrderMap.put(seq, data);
        if (getFreeSpace() == 0) {
            nospace = true;
        }
        return null;
    }

    private void advanceExpSeq() {
        expSeq = (expSeq + 1) % KtpConstants.MAX_SEQ_NUM;
    }

    public synchronized byte[] peekMessage() {
        return inOrderQueue.peekFirst();
    }

    /** Removes and returns the next whole message, or null when empty. */
    public synchronized byte[] readMessage() {
        return inOrderQueue.pollFirst();
    }

    public synchronized KtpPacket checkNospaceRecovery() {
        int free = getFreeSpace();
        long now = System.currentTimeMillis();
        if (nospace && free > 0) {
            nospace = false;
            lastRecoveryAckTime = now;
            return KtpPacket.createAckPacket(lastAckedSeq, free);
        }
        // Window-update ACKs are repeated periodically because one update may
        // itself be lost on the simulated unreliable channel.
        if (hasAck && free > 0 && now - lastRecoveryAckTime >= 1000) {
            lastRecoveryAckTime = now;
            return KtpPacket.createAckPacket(lastAckedSeq, free);
        }
        return null;
    }

    public synchronized KtpPacket createCurrentAck() {
        return KtpPacket.createAckPacket(lastAckedSeq, getFreeSpace());
    }

    public synchronized int getInOrderCount() { return inOrderQueue.size(); }
    public synchronized int getOutOfOrderCount() { return outOfOrderMap.size(); }
    public synchronized int getExpSeq() { return expSeq; }
    public synchronized int getLastAckedSeq() { return lastAckedSeq; }
    public synchronized boolean isNospace() { return nospace; }
}
