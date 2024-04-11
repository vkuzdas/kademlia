package kademlia;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class RoutingTable {
    private static final Logger logger = LoggerFactory.getLogger(RoutingTable.class);
    private final int MAX_SIZE;
    private final int ALPHA_PARAMETER;
    private final int K_PARAMETER;
    private final NodeReference owner;
    List<KBucket> buckets = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private int size; // TODO: decrement

    public RoutingTable(int maxSize, int alpha, int k, NodeReference owner) {
        this.MAX_SIZE = maxSize;
        this.ALPHA_PARAMETER = alpha;
        this.K_PARAMETER = k;
        this.size = 0;

        this.owner = owner;

        for (int i = 0; i < MAX_SIZE; i++) {
            buckets.add(new KBucket(K_PARAMETER, owner));
        }
    }

    public int getSize() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Response to FIND_NODE RPC <br>
     * Recipient returns k nodes it knows about closest to the target ID <br>
     * <a href="https://stackoverflow.com/questions/30654398/implementing-find-node-on-torrent-kademlia-routing-table/30655403#30655403">Link:</a>
     * TL;DR: "just look one bucket left, one bucket right" is not sufficient. The correct algorithm is fairly involved, a linear scan over the whole table is easier to implement
     */
    public List<NodeReference> findKClosest(BigInteger targetId) {
        return buckets.stream()
                .filter(b -> !b.isEmpty())
                .flatMap(KBucket::toStream)
                .sorted(Comparator.comparing(node -> targetId.xor(node.getId())))
                .limit(K_PARAMETER)
                .collect(Collectors.toList());

//        int index = getBucketIndex(targetId);
//
//        KBucket targetBucket = buckets.get(index);
//
//        lock.lock();
//        try {
//            if (targetBucket.isFull()) {
//                return targetBucket.toList();
//            }
//
//            // else keep merging up/down buckets
//            int shift = 1;
//            ArrayList<NodeReference> res = new ArrayList<>(targetBucket.toList());
//
//            while (shift <= Math.max(index, MAX_SIZE - index)) {
//
//                ArrayList<NodeReference> closestCandidates = new ArrayList<>();
//
//                if (index - shift >= 0)
//                    closestCandidates.addAll(getKBucket(index - shift).toList());
//
//                if (index + shift < MAX_SIZE)
//                    closestCandidates.addAll(getKBucket(index + shift).toList());
//
//                // sort by difference, add the remaining closest elements
//                closestCandidates.stream()
//                        .sorted(Comparator.comparing(nodeReference -> targetId.subtract(nodeReference.getId()).abs()))
//                        .limit(K_PARAMETER - res.size())
//                        .forEach(res::add);
//                shift++;
//            }
//
//            return res; // there is less than K nodes
//        } finally {
//            lock.unlock();
//        }

    }

    public void insert(NodeReference newNode) {
        lock.lock();
        try {
            if (owner.equals(newNode)) return;
            int index = getBucketIndex(newNode.getId());
            KBucket bucket = buckets.get(index);

            boolean inserted = bucket.add(newNode);

            if (inserted) {
                size++;
            }
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
        if (owner.getId().compareTo(targetId) == 0)
            return 0;

        BigInteger distance = owner.getId().xor(targetId);

        // returns the index of highest non-zero bit of binary representation
        return distance.bitLength() - 1;
    }

    @VisibleForTesting
    public void print() {
        lock.lock();
        try {
            logger.info("[{}]  Routing table: ", owner);
            for (int i = 0; i < MAX_SIZE; i++) {
                if (buckets.get(i).getSize() == 0) continue;
                logger.info("   Bucket " + i + ": " + buckets.get(i).toList());
            }
        } finally {
            lock.unlock();
        }
    }


}
