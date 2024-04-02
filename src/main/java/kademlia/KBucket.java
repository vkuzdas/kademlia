package kademlia;

import java.time.Instant;
import java.util.ArrayDeque;

/**
 * Each KBucket is kept sorted by time last seen: most-recently seen at the tail
 */
public class KBucket {
    private final int MAX_SIZE;
    private final NodeReference owner;
    private final ArrayDeque<NodeReference> nodes;

//    public KBucket(int k) {
//        this.MAX_SIZE = k;
//        nodes = new ArrayDeque<>();
//    }

    public KBucket(int k, NodeReference owner) {
        this.MAX_SIZE = k;
        this.owner = owner;
        nodes = new ArrayDeque<>();
    }

    // most recently at the tail
    public boolean add(NodeReference newNode) {

        if (owner.equals(newNode)) return false;

        newNode.setLastSeen(Instant.now());

        if (nodes.contains(newNode)) {
            nodes.remove(newNode);
            nodes.addLast(newNode);
            return true;
        }

        if (nodes.size() == MAX_SIZE)  {
            nodes.removeFirst();
            nodes.addLast(newNode);
            return true;
        }

        nodes.addLast(newNode);
        return true;
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
