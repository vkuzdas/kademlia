package kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class RoutingTable {
    private final int SIZE;
    private final int ALPHA_PARAMETER;
    private final int K_PARAMETER;
    private final NodeReference owner;
    List<KBucket> buckets = new ArrayList<>();

    public RoutingTable(int size, int alpha, int k, NodeReference owner) {
        this.SIZE = size;
        this.ALPHA_PARAMETER = alpha;
        this.K_PARAMETER = k;

        this.owner = owner;

        for (int i = 0; i < SIZE; i++) {
            buckets.add(new KBucket(K_PARAMETER, owner));
        }
    }

    public void insert(NodeReference newNode) {
        int index = getBucketIndex(newNode);
        KBucket bucket = buckets.get(index);
        bucket.add(newNode);
    }

    private int getBucketIndex(NodeReference node) {
        BigInteger myId = owner.getDecimalId();
        BigInteger nodeId = node.getDecimalId();

        BigInteger distance = myId.xor(nodeId);

        // returns the index of highest non-zero bit of binary representation
        return distance.bitLength() - 1;
    }


}
