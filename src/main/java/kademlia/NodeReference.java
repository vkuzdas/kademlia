package kademlia;


import com.google.common.annotations.VisibleForTesting;
import proto.Kademlia;

import java.math.BigInteger;
import java.time.Instant;

/**
 * Reference to a PastryNode from the point of view of current node
 */
public class NodeReference {
    public final String ip;
    public final int port;
    private /*final*/ BigInteger id;
    private Instant lastSeen;

    public NodeReference(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.id = Util.getId(getAddress());
    }

    public NodeReference(String ip, int port, BigInteger id) {
        this.ip = ip;
        this.port = port;
        this.id = id;
    }

    public NodeReference(Kademlia.NodeReference nodeReference) {
        this.ip = nodeReference.getIp();
        this.port = nodeReference.getPort();
        this.id = new BigInteger(nodeReference.getId());
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

    public BigInteger getId() {
        return id;
    }

    public String getAddress() {
        return ip + ":" + port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    @VisibleForTesting
    public void setId(BigInteger id) {
        this.id = id;
    }


    @Override
    public String toString() {
        return
//                ip + ":" +
                        port + ":" + id + ":" + getId();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof NodeReference)) {
            return false;
        }
        NodeReference other = (NodeReference) obj;
        return this.port == other.port && this.ip.equals(other.ip);
    }

    @Override
    public int hashCode() {
        int result = 17; // Základní hodnota pro výpočet hash kódu
        result = 31 * result + ip.hashCode(); // Použití IP adresy pro výpočet hash kódu
        result = 31 * result + port; // Použití portu pro výpočet hash kódu
        return result;
    }

}
