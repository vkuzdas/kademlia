package kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class RoutingTable {
    private final int SIZE;
    private final int ALPHA_PARAMETER;
    private final int K_PARAMETER;
    private final NodeReference owner;
    List<KBucket> buckets = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();

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
    // assumption: pokud full -> vratim K (tzn pokud je targetId na okraji, nevratim nutne ty skutecne nejblizsi)
    //      pokud not-full, vratim ty skutecne nejblizsi z okolnich
    public List<NodeReference> findKClosest(BigInteger targetId) {
        int index = getBucketIndex(targetId);

        KBucket targetBucket = buckets.get(index);

        lock.lock();
        try {
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
                        .sorted(Comparator.comparing(nodeReference -> targetId.subtract(nodeReference.getId()).abs()))
                        .limit(K_PARAMETER - res.size())
                        .forEach(res::add);
            }

            return res; // there is less than K nodes
        } finally {
            lock.unlock();
        }

    }

    public void insert(NodeReference newNode) {
        lock.lock();
        try {
            if (owner.equals(newNode)) return;
            int index = getBucketIndex(newNode.getId());
            KBucket bucket = buckets.get(index);
            bucket.add(newNode);
        } finally {
            lock.unlock();
        }
    }

    public KBucket getKBucket(int index) {
        lock.lock();
        try {
            return buckets.get(index);
        } finally {
            lock.unlock();
        }
    }

    public int getBucketIndex(BigInteger targetId) {
        lock.lock();
        try {
            BigInteger distance = owner.getId().xor(targetId);

            // returns the index of highest non-zero bit of binary representation
            return distance.bitLength() - 1;
        } finally {
            lock.unlock();
        }
    }


}
