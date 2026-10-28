import java.nio.ByteBuffer;

/** KTP wire packet with a fixed header and a payload of at most 512 bytes. */
public class KtpPacket {
    public static final int HEADER_SIZE = 16;

    public byte type;
    public int seqNum;
    public int ackNum;
    public int rwnd;
    public int len;
    public int checksum;
    public long timestamp;
    public byte[] payload;

    public int transmitCount = 0;
    public long firstSentTime = 0;
    public long lastSentTime = 0;

    public KtpPacket() {
        payload = new byte[0];
    }

    public KtpPacket(byte type, int seqNum, int ackNum, int rwnd, byte[] data, int offset, int length) {
        if (length < 0 || length > KtpConstants.MAX_MESSAGE_SIZE
                || (length > 0 && (data == null || offset < 0 || offset > data.length - length))) {
            throw new IllegalArgumentException("Invalid KTP payload length or offset");
        }
        this.type = type;
        this.seqNum = seqNum & 0xFF;
        this.ackNum = ackNum & 0xFF;
        this.rwnd = Math.max(0, Math.min(rwnd, KtpConstants.WINDOW_SIZE));
        this.len = length;
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[length];
        if (data != null && length > 0) System.arraycopy(data, offset, payload, 0, length);
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
        if (payload != null) for (byte b : payload) sum += b & 0xFF;
        return sum & 0xFFFF;
    }

    public boolean verifyChecksum() {
        return checksum == calculateChecksum();
    }

    public byte[] toBytes() {
        checksum = calculateChecksum();
        ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE + len);
        bb.put(type);
        bb.put((byte) (seqNum & 0xFF));
        bb.put((byte) (ackNum & 0xFF));
        bb.put((byte) (rwnd & 0xFF));
        bb.putShort((short) len);
        bb.putShort((short) checksum);
        bb.putLong(timestamp);
        if (payload != null && len > 0) bb.put(payload, 0, len);
        return bb.array();
    }

    /**
     * Returns null for malformed packets rather than silently converting a bad
     * payload length into a valid empty packet.
     */
    public static KtpPacket fromBytes(byte[] raw, int offset, int length) {
        if (raw == null || offset < 0 || length < HEADER_SIZE || offset > raw.length - length) return null;

        ByteBuffer bb = ByteBuffer.wrap(raw, offset, length);
        KtpPacket pkt = new KtpPacket();
        pkt.type = bb.get();
        pkt.seqNum = bb.get() & 0xFF;
        pkt.ackNum = bb.get() & 0xFF;
        pkt.rwnd = bb.get() & 0xFF;
        pkt.len = bb.getShort() & 0xFFFF;
        pkt.checksum = bb.getShort() & 0xFFFF;
        pkt.timestamp = bb.getLong();

        if (pkt.type < KtpConstants.TYPE_DATA || pkt.type > KtpConstants.TYPE_FIN_ACK
                || pkt.rwnd > KtpConstants.WINDOW_SIZE
                || pkt.len > KtpConstants.MAX_MESSAGE_SIZE
                || length != HEADER_SIZE + pkt.len) return null;
        if (pkt.type == KtpConstants.TYPE_DATA || pkt.type == KtpConstants.TYPE_RETRANSMIT) {
            if (pkt.len == 0) return null;
        } else if (pkt.len != 0) {
            return null;
        }

        pkt.payload = new byte[pkt.len];
        if (pkt.len > 0) bb.get(pkt.payload);
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
