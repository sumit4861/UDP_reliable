import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * ReceiveWindow maintains the receiver-side message buffer, handles in-order
 * and out-of-order packets, duplicates, and manages rwnd flow control advertisement.
 */
public class ReceiveWindow {
    private final int windowCapacity = KtpConstants.WINDOW_SIZE;
    private final LinkedList<byte[]> inOrderQueue = new LinkedList<>();
    private final Map<Integer, byte[]> outOfOrderMap = new HashMap<>();

    private int expSeq = 1;          // Expected in-order sequence number (starts at 1)
    private int lastAckedSeq = 0;    // Last in-order acknowledged sequence number
    private boolean nospace = false; // Flag set when available buffer space is zero
    private long lastRecoveryAckTime = 0;

    /**
     * Computes the current free space in the receiver buffer.
     * rwnd size is equivalent to the amount of free space in the receiver buffer.
     */
    public synchronized int getFreeSpace() {
        int used = inOrderQueue.size() + outOfOrderMap.size();
        return Math.max(0, windowCapacity - used);
    }

    /**
     * Processes an incoming DATA or RETRANSMIT packet.
     * Returns an ACK packet to send back, or null if no ACK should be sent.
     */
    public synchronized KtpPacket processDataPacket(KtpPacket pkt) {
        int seq = pkt.seqNum;
        int dist = (seq - expSeq + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;
        int freeSpace = getFreeSpace();

        // 1. Check for Duplicate Packet
        if (dist >= 200 || outOfOrderMap.containsKey(seq)) {
            if (lastAckedSeq > 0) {
                return KtpPacket.createAckPacket(lastAckedSeq, freeSpace);
            }
            return null;
        }

        // Check if outside current window
        if (dist >= freeSpace) {
            if (lastAckedSeq > 0) {
                return KtpPacket.createAckPacket(lastAckedSeq, freeSpace);
            }
            return null;
        }

        // 2. In-order Packet
        if (dist == 0) {
            // Store message payload in in-order queue
            byte[] msgData = new byte[pkt.len];
            System.arraycopy(pkt.payload, 0, msgData, 0, pkt.len);
            inOrderQueue.addLast(msgData);

            // Advance expected sequence number
            advanceExpSeq();

            // Check if subsequent contiguous packets are already buffered out-of-order
            while (outOfOrderMap.containsKey(expSeq)) {
                byte[] bufferedData = outOfOrderMap.remove(expSeq);
                inOrderQueue.addLast(bufferedData);
                advanceExpSeq();
            }

            lastAckedSeq = (expSeq - 1 + KtpConstants.MAX_SEQ_NUM) % KtpConstants.MAX_SEQ_NUM;

            int updatedRwnd = getFreeSpace();
            if (updatedRwnd == 0) {
                nospace = true;
            }

            // Return ACK piggybacking updated rwnd and last in-order seq
            return KtpPacket.createAckPacket(lastAckedSeq, updatedRwnd);
        }

        // 3. Out-of-order Packet
        if (dist > 0 && dist < freeSpace) {
            if (!outOfOrderMap.containsKey(seq)) {
                byte[] msgData = new byte[pkt.len];
                System.arraycopy(pkt.payload, 0, msgData, 0, pkt.len);
                outOfOrderMap.put(seq, msgData);
            }
            if (getFreeSpace() == 0) {
                nospace = true;
            }
            // PDF: "If the receiver receives an out of order message, it keeps the message in the buffer
            // (if the message sequence number is within rwnd) but does not send any ACK message."
            return null;
        }

        // Outside window or buffer full
        return null;
    }

    private void advanceExpSeq() {
        expSeq = (expSeq + 1) % KtpConstants.MAX_SEQ_NUM;
    }

    /**
     * Reads the next in-order message from the receive buffer (k_recvfrom).
     * Returns payload bytes or null if no message is available.
     */
    public synchronized byte[] readMessage() {
        if (inOrderQueue.isEmpty()) {
            return null;
        }
        return inOrderQueue.removeFirst();
    }

    /**
     * Checks if the nospace flag was set, but now space is available in the receive buffer.
     * In that case, sends an ACK with last acknowledged sequence number and updated rwnd size.
     */
    public synchronized KtpPacket checkNospaceRecovery() {
        int freeSpace = getFreeSpace();
        long now = System.currentTimeMillis();

        if (nospace && freeSpace > 0) {
            nospace = false;
            lastRecoveryAckTime = now;
            return KtpPacket.createAckPacket(lastAckedSeq, freeSpace);
        } else if (!nospace && freeSpace > 0 && (now - lastRecoveryAckTime > 1000) && lastRecoveryAckTime > 0) {
            // Periodic window update to avoid deadlock in case earlier update was lost
            lastRecoveryAckTime = now;
            return KtpPacket.createAckPacket(lastAckedSeq, freeSpace);
        }
        return null;
    }

    public synchronized KtpPacket createCurrentAck() {
        return KtpPacket.createAckPacket(lastAckedSeq, getFreeSpace());
    }

    public synchronized int getInOrderCount() {
        return inOrderQueue.size();
    }

    public synchronized int getOutOfOrderCount() {
        return outOfOrderMap.size();
    }

    public synchronized int getExpSeq() {
        return expSeq;
    }

    public synchronized int getLastAckedSeq() {
        return lastAckedSeq;
    }

    public synchronized boolean isNospace() {
        return nospace;
    }
}
