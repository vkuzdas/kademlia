package kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
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

    /**
     * Response to FIND_NODE RPC
     * Recepient returns k nodes it knows about closest to the target ID
     */

    public List<NodeReference> findKClosest(BigInteger targetId) {
        int index = getBucketIndex(targetId);

        KBucket targetBucket = buckets.get(index);

        if (targetBucket.isFull()) {
            return targetBucket.toList();
        }

        int indexUp = index;
        int indexDown = index;

        ArrayList<NodeReference> res = new ArrayList<>(targetBucket.toList());

        while (indexUp != SIZE && indexDown != 0) {
            indexUp++;
            indexDown--;

            ArrayList<NodeReference> closestCandidates = new ArrayList<>();

            if (indexDown >= 0)
                closestCandidates.addAll(getKBucket(indexDown).toList());

            if (indexUp < SIZE)
                closestCandidates.addAll(getKBucket(indexUp).toList());

            // sort by difference, add the remaining closest elements
            closestCandidates.stream()
                    .sorted(Comparator.comparing(nodeReference -> targetId.subtract(nodeReference.getDecimalId()).abs()))
                    .limit(K_PARAMETER - res.size())
                    .forEach(res::add);
        }

        return res; // there is less than K nodes
    }

    public void insert(NodeReference newNode) {
        if (owner.equals(newNode)) return;
        int index = getBucketIndex(newNode.getDecimalId());
        KBucket bucket = buckets.get(index);
        bucket.add(newNode);
    }

    public KBucket getKBucket(int index) {
        return buckets.get(index);
    }

    private int getBucketIndex(BigInteger targetId) {
        BigInteger distance = owner.getDecimalId().xor(targetId);

        // returns the index of highest non-zero bit of binary representation
        return distance.bitLength() - 1;
    }


}
