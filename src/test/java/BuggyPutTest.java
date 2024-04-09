import kademlia.KademliaNode;
import kademlia.NodeReference;
import kademlia.Util;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static kademlia.Util.*;
import static org.junit.jupiter.api.Assertions.*;


public class BuggyPutTest extends BaseTest {

    @Test
    public void testPut_nonOverlapping_scratch_12() throws IOException {
        List<NodeReference> lowRange = new ArrayList<>();
        List<NodeReference> highRange = new ArrayList<>();

        KademliaNode joiner1 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(0)));
        joiner1.initKademlia();
        lowRange.add(joiner1.getNodeReference());
        runningNodes.add(joiner1);

        KademliaNode joiner2 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(1)));
        joiner2.join(joiner1.getNodeReference());
        lowRange.add(joiner2.getNodeReference());
        runningNodes.add(joiner2);

        KademliaNode joiner3 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(2)));
        joiner3.join(joiner2.getNodeReference());
        lowRange.add(joiner3.getNodeReference());
        runningNodes.add(joiner3);

        KademliaNode joiner4 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(3)));
        joiner4.join(joiner2.getNodeReference());
        lowRange.add(joiner4.getNodeReference());
        runningNodes.add(joiner4);

        // high nodes

        KademliaNode joiner5 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(0)));
        joiner5.join(joiner1.getNodeReference());
        highRange.add(joiner5.getNodeReference());
        runningNodes.add(joiner5);

        KademliaNode joiner6 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(1)));
        joiner6.join(joiner1.getNodeReference());
        highRange.add(joiner6.getNodeReference());
        runningNodes.add(joiner6);

        KademliaNode joiner7 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(2)));
        joiner7.join(joiner1.getNodeReference());
        highRange.add(joiner7.getNodeReference());
        runningNodes.add(joiner7);

        KademliaNode joiner8 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(3)));
        joiner8.join(joiner1.getNodeReference());
        highRange.add(joiner8.getNodeReference());
        runningNodes.add(joiner8);


        String lowKey = "key3"; // keyHash=885733 ∈ [2^19, 2^20)
        String highKey = "key1"; // keyhash= 40604 ∈ [2^15, 2^16)

        joiner3.put(highKey, "highValue");
        System.out.println("smtn wong");



//        joiner7.put(lowKey, "lowValue");
//
//        List<Pair> expectLowRange = getRandomRunningNode().get(lowKey);
//        List<Pair> expectHighRange = getRandomRunningNode().get(highKey);
//
//
//        expectLowRange.forEach(p -> assertTrue(lowRange.contains(p.node)));
//        expectHighRange.forEach(p -> assertTrue(highRange.contains(p.node)));
//
//        for (int i = 0; i < K; i++) {
//            assertEquals("lowValue", runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
//            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
//        }
//        for (int i = K; i < 2*K; i++) {
//            assertEquals("highValue", runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
//            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
//        }

    }
//
//    @Test
//    public void testPut_nonOverlapping_scratch5() throws IOException {
//        List<NodeReference> lowRange = new ArrayList<>();
//        List<NodeReference> highRange = new ArrayList<>();
//
//        KademliaNode joiner1 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(0)));
//        joiner1.initKademlia();
//        lowRange.add(joiner1.getNodeReference());
//        runningNodes.add(joiner1);
//
//        KademliaNode joiner2 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(1)));
//        joiner2.join(joiner1.getNodeReference());
//        lowRange.add(joiner2.getNodeReference());
//        runningNodes.add(joiner2);
//
//        KademliaNode joiner3 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(2)));
//        joiner3.join(joiner2.getNodeReference());
//        lowRange.add(joiner3.getNodeReference());
//        runningNodes.add(joiner3);
//
//        KademliaNode joiner4 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(3)));
//        joiner4.join(joiner1.getNodeReference());
//        lowRange.add(joiner4.getNodeReference());
//        runningNodes.add(joiner4);
//
//        // high nodes
//
//        KademliaNode joiner5 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(0)));
//        joiner5.join(joiner3.getNodeReference());
//        highRange.add(joiner5.getNodeReference());
//        runningNodes.add(joiner5);
//
//        KademliaNode joiner6 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(1)));
//        joiner6.join(joiner3.getNodeReference());
//        highRange.add(joiner6.getNodeReference());
//        runningNodes.add(joiner6);
//
//        KademliaNode joiner7 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(2)));
//        joiner7.join(joiner4.getNodeReference());
//        highRange.add(joiner7.getNodeReference());
//        runningNodes.add(joiner7);
//
//        KademliaNode joiner8 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(3)));
//        joiner8.join(joiner5.getNodeReference());
//        highRange.add(joiner8.getNodeReference());
//        runningNodes.add(joiner8);
//
//
//        String lowKey = "key3"; // keyHash=885733 ∈ [2^19, 2^20)
//        String highKey = "key1"; // keyhash= 40604 ∈ [2^15, 2^16)
//
//        joiner3.put(highKey, "highKey");
//
//        joiner7.put(lowKey, "lowKey");
//
//        List<Pair> expectHighRange = getRandomRunningNode().get("key1");
//        List<Pair> expectLowRange = getRandomRunningNode().get("key3");
//
//
//        expectLowRange.forEach(p -> assertTrue(lowRange.contains(p.node)));
//        expectHighRange.forEach(p -> assertTrue(highRange.contains(p.node)));
//
//        for (int i = 1; i < K; i++) {
//            assertEquals("lowKey", runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
//            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
//        }
//        for (int i = K; i < 2*K; i++) {
//            assertEquals("highKey", runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
//            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
//        }
//
//    }
//
//    @Test
//    public void repeate() throws IOException {
//        for (int i = 0; i < 10; i++) {
//            testPut_nonOverlapping_random();
//            tearDown();
//        }
//    }
//
//    @Test
//    public void testPut_nonOverlapping_random() throws IOException {
//        List<NodeReference> lowRange = new ArrayList<>();
//        List<NodeReference> highRange = new ArrayList<>();
//
//        // low nodes
//        for (int i = 0; i < K; i++) {
//            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(i)));
//
//            if (runningNodes.isEmpty()) {
//                joiner.initKademlia();
//            } else {
//                joiner.join(getRandomRunningNode().getNodeReference());
//            }
//
//            lowRange.add(joiner.getNodeReference());
//            runningNodes.add(joiner);
//        }
//
//        // high nodes
//        for (int i = 0; i < K; i++) {
//            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(i)));
//
//            joiner.join(runningNodes.get(0).getNodeReference());
//
//            highRange.add(joiner.getNodeReference());
//            runningNodes.add(joiner);
//        }
//
//        String lowKey = "key3"; // keyHash=885733 ∈ [2^19, 2^20)
//        String highKey = "key1"; // keyhash= 40604 ∈ [2^15, 2^16)
//
//        getRandomRunningNode().put(highKey, "highKey");
//
//        getRandomRunningNode().put(lowKey, "lowKey");
//
//        List<Pair> expectHighRange = getRandomRunningNode().get("key1");
//        List<Pair> expectLowRange = getRandomRunningNode().get("key3");
//
//
//        expectLowRange.forEach(p -> assertTrue(lowRange.contains(p.node)));
//        expectHighRange.forEach(p -> assertTrue(highRange.contains(p.node)));
//
//        for (int i = 1; i < K; i++) {
//            assertEquals("lowKey", runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
//            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
//        }
//        for (int i = K; i < 2*K; i++) {
//            assertEquals("highKey", runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
//            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
//        }
//    }
}
