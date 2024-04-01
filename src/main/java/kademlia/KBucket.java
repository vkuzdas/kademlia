package kademlia;

import java.util.ArrayDeque;

/**
 * Each KBucket is kept sorted by time last seen: most-recently seen at the tail
 */
public class KBucket {
    private final int MAX_SIZE;

    private final ArrayDeque<NodeReference> nodes;

    // TODO: should I add owner to prevent owner insertion on this level?
    public KBucket(int k) {
        this.MAX_SIZE = k;
        nodes = new ArrayDeque<>();
    }

    public boolean addTail(NodeReference node) {
        if (nodes.size() < MAX_SIZE) {
            nodes.addLast(node);
            return true;
        }
        return false;
    }

    public boolean addHead(NodeReference node) {
        if (nodes.size() < MAX_SIZE) {
            nodes.addFirst(node);
            return true;
        }
        return false;
    }

    public boolean contains(NodeReference node) {
        return nodes.contains(node);
    }

    public boolean isFull() {
        return nodes.size() == MAX_SIZE;
    }

    public NodeReference getHead() {
        return nodes.peekFirst();
    }

    public NodeReference getTail() {
        return nodes.peekLast();
    }
}
