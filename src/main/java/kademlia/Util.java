package kademlia;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Util {

    static int bits = 8;
    static int K_PARAMETER = 5;

    /**
     * Calculate the SHA-1 hash of the input string and return the result as a BigInteger
     */
    private static BigInteger calculateSHA1(String input) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace(System.err);
            return BigInteger.ONE.negate();
        }

        BigInteger lastId = BigInteger.valueOf(2L).pow(bits).add(BigInteger.ONE.negate()); // [0, 2^x-1]
        BigInteger hashedString = new BigInteger(1, md.digest(input.getBytes(StandardCharsets.UTF_8)));

        return hashedString.mod(lastId);
    }

    public static String decToBin(BigInteger input) {
        return input.toString(2);
    }

    public static BigInteger getId(String address) {
        return calculateSHA1(address);
    }
}
