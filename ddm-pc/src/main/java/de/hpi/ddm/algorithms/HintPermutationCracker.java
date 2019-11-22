package de.hpi.ddm.algorithms;

import de.hpi.ddm.structures.SHA256Hash;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Cracks a set of hints given their SHA256 hash, by a permutation algorithm of a given character set.
 */
public class HintPermutationCracker {
    private final byte[] choices;
    private final byte[] prefix;

    public HintPermutationCracker(String choices, String prefix) {
        if (choices == null || choices.length() == 0)
            throw new IllegalArgumentException("HintPermutationCracker: 'choices' must be a non-null, non-empty string.");
        if (!StandardCharsets.US_ASCII.newEncoder().canEncode(choices))
            throw new IllegalArgumentException("HintPermutationCracker: 'choices' can only contain ASCII characters.");
        if (prefix != null && !StandardCharsets.US_ASCII.newEncoder().canEncode(prefix))
            throw new IllegalArgumentException("HintPermutationCracker: 'choices' can only contain ASCII characters.");

        this.choices = choices.getBytes(StandardCharsets.US_ASCII);
        this.prefix = prefix != null ? prefix.getBytes(StandardCharsets.US_ASCII) : new byte[0];
    }

    /**
     * Generates a new permutation in place using the "Countdown QuickPerm Algorithm" developed by Phillip Paul Fuchs
     * This is the algorithm used in hashcat-utils permute.c
     * See: https://github.com/hashcat/hashcat-utils/blob/f2a86c76c7ce38ebfeb6ea4a16b5dacd6c942afe/src/permute.c
     */
    private static int getNextPermutation(byte[] word, int prefixLength, int[] p, int k) {
        p[k]--;

        int j = (k % 2) * p[k];

        byte tmp = word[prefixLength+j];
        word[prefixLength+j] = word[prefixLength+k];
        word[prefixLength+k] = tmp;

        for (k = 1; p[k] == 0; k++)
            p[k] = k;

        return k;
    }

    /**
     * Checks if the SHA256 hash of the given permutation matches some of the hints to crack,
     * and if so, stores the cracked hint information (plain text and missing character) in the map.
     * @param candidate The permutation corresponding to the hint.
     * @param hintHashes Set of hint SHA256 hashes that we are trying to crack.
     * @param crackedHints Map of cracked hint SHA256 hashes to character missing from the hint.
     */
    private static void tryCrackPasswordHint(byte[] candidate, Set<SHA256Hash> hintHashes, Map<SHA256Hash, Character> crackedHints) {
        // Ignore the last character of the permutation (since it is not in the hint, just used for the permutations!)
        // In fact, note that in case of a match, this last character will be the character missing in the hint
        SHA256Hash candidateHash = SHA256Hash.fromDataHash(candidate, candidate.length - 1);

        if (hintHashes.contains(candidateHash)) {
            // Save the cracked hint plain text
            crackedHints.put(candidateHash, (char) candidate[candidate.length - 1]);
        }
    }

    /**
     * Cracks all the hint hashes in the given array using this instance's hint permutation cracker configuration.
     * @param hintHashes Array of hint hashes to crack.
     * @return A map containing the hint hashes as the key and the character missing in the hint as the value.
     *         (NOTE: possibly not all hints could be cracked, in this case, the hint hash will not be in the map!)
     */
    public Map<SHA256Hash, Character> crack(Set<SHA256Hash> hintHashes) {
        if (hintHashes == null)
            throw new IllegalArgumentException("HintPermutationCracker: 'hintHashes' must be a non-null array.");

        Map<SHA256Hash, Character> crackedHints = new HashMap<>();

        // Initialize the state of the "Countdown QuickPerm Algorithm" (see getNextPermutation for more information)
        int[] p = new int[choices.length + 1];
        for (int k = 0; k < p.length; k++)
            p[k] = k;

        byte[] permutation = new byte[prefix.length + choices.length];
        System.arraycopy(prefix, 0, permutation, 0, prefix.length);
        System.arraycopy(choices, 0, permutation, prefix.length, choices.length);

        // Iterate over all permutations and repeatedly check if they match the corresponding hint hashes
        int k = 1;
        tryCrackPasswordHint(permutation, hintHashes, crackedHints);

        while ((k = getNextPermutation(permutation, prefix.length, p, k)) != choices.length) {
            tryCrackPasswordHint(permutation, hintHashes, crackedHints);
        }

        tryCrackPasswordHint(permutation, hintHashes, crackedHints);

        return crackedHints;
    }
}
