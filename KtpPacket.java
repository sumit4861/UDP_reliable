import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * KTP Packet structure for transport layer communication.
 * Includes header fields for type, sequence number, piggybacked ACK,
 * receiver window (rwnd), length, checksum, and timestamp.
 */
public class KtpPacket {
    public static final int HEADER_SIZE = 16;

    public byte type;         // 0: DATA, 1: RETRANSMIT, 2: ACK, 3: PROBE, 4: FIN, 5: FIN_ACK
    public int seqNum;        // 8-bit sequence number (0..255)
    public int ackNum;        // 8-bit piggybacked acknowledgement number (0..255)
    public int rwnd;          // 8-bit receiver window advertisement (0..WINDOW_SIZE)
    public int len;           // Payload length (0..MAX_MESSAGE_SIZE)
    public int checksum;      // 16-bit checksum
    public long timestamp;    // Send timestamp in milliseconds
    public byte[] payload;    // Payload data (up to MAX_MESSAGE_SIZE)

    // Internal tracking for retransmission count and send time
    public int transmitCount = 0;
    public long firstSentTime = 0;
    public long lastSentTime = 0;

    public KtpPacket() {
        this.payload = new byte[0];
    }

    public KtpPacket(byte type, int seqNum, int ackNum, int rwnd, byte[] data, int offset, int length) {
        this.type = type;
        this.seqNum = seqNum & 0xFF;
        this.ackNum = ackNum & 0xFF;
        this.rwnd = rwnd & 0xFF;
        this.len = length;
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[length];
        if (data != null && length > 0) {
            System.arraycopy(data, offset, this.payload, 0, length);
        }
        this.checksum = calculateChecksum();
    }

    public static KtpPacket createDataPacket(int seqNum, byte[] data, int length) {
        return new KtpPacket(KtpConstants.TYPE_DATA, seqNum, 0, 0, data, 0, length);
    }

    public static KtpPacket createAckPacket(int ackNum, int rwnd) {
        return new KtpPacket(KtpConstants.TYPE_ACK, 0, ackNum, rwnd, null, 0, 0);
    }

    public static KtpPacket createWindowProbe(int seqNum) {
        return new KtpPacket(KtpConstants.TYPE_WINDOW_PROBE, seqNum, 0, 0, null, 0, 0);
    }

    public static KtpPacket createFinPacket(int seqNum) {
        return new KtpPacket(KtpConstants.TYPE_FIN, seqNum, 0, 0, null, 0, 0);
    }

    public static KtpPacket createFinAckPacket(int ackNum) {
        return new KtpPacket(KtpConstants.TYPE_FIN_ACK, 0, ackNum, 0, null, 0, 0);
    }

    public int calculateChecksum() {
        int sum = (type & 0xFF) + (seqNum & 0xFF) + (ackNum & 0xFF) + (rwnd & 0xFF) + len;
        if (payload != null) {
            for (byte b : payload) {
                sum += (b & 0xFF);
            }
        }
        return sum & 0xFFFF;
    }

    public boolean verifyChecksum() {
        return this.checksum == calculateChecksum();
    }

    public byte[] toBytes() {
        this.checksum = calculateChecksum();
        ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE + len);
        bb.put(type);
        bb.put((byte) (seqNum & 0xFF));
        bb.put((byte) (ackNum & 0xFF));
        bb.put((byte) (rwnd & 0xFF));
        bb.putShort((short) len);
        bb.putShort((short) checksum);
        bb.putLong(timestamp);
        if (payload != null && len > 0) {
            bb.put(payload, 0, len);
        }
        return bb.array();
    }

    public static KtpPacket fromBytes(byte[] raw, int offset, int length) {
        if (length < HEADER_SIZE) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(raw, offset, length);
        KtpPacket pkt = new KtpPacket();
        pkt.type = bb.get();
        pkt.seqNum = bb.get() & 0xFF;
        pkt.ackNum = bb.get() & 0xFF;
        pkt.rwnd = bb.get() & 0xFF;
        pkt.len = bb.getShort() & 0xFFFF;
        pkt.checksum = bb.getShort() & 0xFFFF;
        pkt.timestamp = bb.getLong();

        if (pkt.len > 0 && pkt.len <= (length - HEADER_SIZE)) {
            pkt.payload = new byte[pkt.len];
            bb.get(pkt.payload, 0, pkt.len);
        } else {
            pkt.payload = new byte[0];
            pkt.len = 0;
        }
        return pkt;
    }

    @Override
    public String toString() {
        String typeStr;
        switch (type) {
            case KtpConstants.TYPE_DATA: typeStr = "DATA"; break;
            case KtpConstants.TYPE_RETRANSMIT: typeStr = "RETX"; break;
            case KtpConstants.TYPE_ACK: typeStr = "ACK"; break;
            case KtpConstants.TYPE_WINDOW_PROBE: typeStr = "PROBE"; break;
            case KtpConstants.TYPE_FIN: typeStr = "FIN"; break;
            case KtpConstants.TYPE_FIN_ACK: typeStr = "FIN_ACK"; break;
            default: typeStr = "UNKNOWN(" + type + ")"; break;
        }
        return String.format("[KtpPacket %s seq=%d ack=%d rwnd=%d len=%d]",
                typeStr, seqNum, ackNum, rwnd, len);
    }
}
