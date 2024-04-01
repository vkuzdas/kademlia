package kademlia;


import java.math.BigInteger;
import java.time.Instant;

/**
 * Reference to a PastryNode from the point of view of current node
 */
public class NodeReference {
    private final String ip;
    private final int port;
    private final BigInteger decimalId;
    private Instant lastSeen;

    public NodeReference(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.decimalId = Util.getId(getAddress());
    }

    public void setLastSeen(Instant lastSeen) {
        this.lastSeen = lastSeen;
    }

    public Instant getLastSeen() {
        return lastSeen;
    }

    public void updateLastSeenNow() {
        lastSeen = Instant.now();
    }

    public BigInteger getDecimalId() {
        return decimalId;
    }

    public String getAddress() {
        return ip + ":" + port;
    }


    @Override
    public String toString() {
        return
//                ip + ":" +
                        port + ":" + decimalId + ":" + getDecimalId();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof NodeReference)) {
            return false;
        }
        NodeReference other = (NodeReference) obj;
        return this.port == other.port && this.ip.equals(other.ip);
    }
}
