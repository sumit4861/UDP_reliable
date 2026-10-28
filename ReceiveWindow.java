import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/** Receiver-side KTP window with ordered delivery and bounded reordering storage. */
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

    /** Out-of-order packets are buffered within the receive sequence window but not ACKed. */
    public synchronized KtpPacket processDataPacket(KtpPacket pkt) {
        if (pkt == null || pkt.seqNum < 0 || pkt.seqNum >= KtpConstants.MAX_SEQ_NUM
                || pkt.len <= 0 || pkt.len > KtpConstants.MAX_MESSAGE_SIZE
                || pkt.payload == null || pkt.payload.length != pkt.len) {
            return hasAck ? KtpPacket.createAckPacket(lastAckedSeq, getFreeSpace()) : null;
        }

        int seq = pkt.seqNum;
        int dist = (seq - expSeq + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;
        int free = getFreeSpace();

        // The valid sequence window is bounded by its capacity, not by the
        // instantaneous free count; buffer occupancy is checked separately.
        if (dist >= windowCapacity || free == 0) {
            return hasAck ? KtpPacket.createAckPacket(lastAckedSeq, free) : null;
        }

        if (dist == 0) {
            byte[] data = pkt.payload.clone();
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

        // A packet already buffered is a duplicate; do not store it twice.
        if (outOfOrderMap.containsKey(seq)) {
            return hasAck ? KtpPacket.createAckPacket(lastAckedSeq, free) : null;
        }

        // Old/stale sequence numbers map near the end of the modulo space.
        if (dist >= KtpConstants.MAX_SEQ_NUM / 2) {
            return hasAck ? KtpPacket.createAckPacket(lastAckedSeq, free) : null;
        }

        outOfOrderMap.put(seq, pkt.payload.clone());
        if (getFreeSpace() == 0) nospace = true;
        return null;
    }

    private void advanceExpSeq() {
        expSeq = (expSeq + 1) % KtpConstants.MAX_SEQ_NUM;
    }

    public synchronized byte[] peekMessage() {
        return inOrderQueue.peekFirst();
    }

    /** Removes and returns a whole message, or null if none is available. */
    public synchronized byte[] readMessage() {
        byte[] message = inOrderQueue.pollFirst();
        if (message != null && getFreeSpace() > 0) nospace = true;
        return message;
    }

    public synchronized KtpPacket checkNospaceRecovery() {
        int free = getFreeSpace();
        long now = System.currentTimeMillis();
        if (nospace && free > 0) {
            nospace = false;
            lastRecoveryAckTime = now;
            return KtpPacket.createAckPacket(lastAckedSeq, free);
        }
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
