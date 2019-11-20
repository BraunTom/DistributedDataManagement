package de.hpi.ddm.algorithms;

import de.hpi.ddm.structures.SHA256Hash;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.ArrayUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HintPermutationCracker {
    private final byte[] choices;
    private final byte[] prefix;

    @Value  @AllArgsConstructor
    public static class CrackedHint {
        String plainText;
        char missingCharacter;
    }

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
     * @param hintsToCrack Map of hint SHA256 hashes to cracked hint information.
     */
    private static void tryCrackPasswordHint(byte[] candidate, Map<SHA256Hash, CrackedHint> hintsToCrack) {
        // Ignore the last character of the permutation (since it is not in the hint, just used for the permutations!)
        // In fact, note that in case of a match, this last character will be the character missing in the hint
        SHA256Hash candidateHash = SHA256Hash.fromDataHash(candidate, candidate.length - 1);

        if (hintsToCrack.containsKey(candidateHash)) {
            // Save the cracked hint plain text
            hintsToCrack.replace(candidateHash, new CrackedHint(
                    new String(candidate, 0, candidate.length - 1, StandardCharsets.UTF_8),
                    (char) candidate[candidate.length - 1]
            ));
        }
    }

    /**
     * Tries to crack (simultaneously) all the hint SHA256 hashes in the map by running a brute force search
     * over the permutations of the password characters.
     * @param hintsToCrack Map of hint SHA256 hashes to cracked hint information. The value will be filled on hash match.
     */
    private void bruteForcePermutations(Map<SHA256Hash, CrackedHint> hintsToCrack) {
        // Initialize the state of the "Countdown QuickPerm Algorithm" (see getNextPermutation for more information)
        int[] p = new int[choices.length + 1];
        for (int k = 0; k < p.length; k++)
            p[k] = k;

        byte[] permutation = ArrayUtils.addAll(prefix, choices);

        // Iterate over all permutations and repeatedly check if they match the corresponding hint hashes
        int k = 1;
        tryCrackPasswordHint(permutation, hintsToCrack);

        while ((k = getNextPermutation(permutation, prefix.length, p, k)) != choices.length) {
            tryCrackPasswordHint(permutation, hintsToCrack);
        }

        tryCrackPasswordHint(permutation, hintsToCrack);
    }

    /**
     * Cracks all the hint hashes in the given array using this instance's hint permutation cracker configuration.
     * @param hintHashes Array of hint hashes to crack.
     * @return A map containing the hint hashes as the key and the cracked hint details as the value.
     *         (NOTE: possibly not all hints could be cracked, in this case, the value in the map will be null!)
     */
    public Map<SHA256Hash, CrackedHint> crack(SHA256Hash[] hintHashes) {
        if (hintHashes == null)
            throw new IllegalArgumentException("HintPermutationCracker: 'hintHashes' must be a non-null array.");

        // Put the hint hashes in a map which will allow efficient membership checking
        Map<SHA256Hash, CrackedHint> crackedHints = new HashMap<>();
        for (SHA256Hash hintHash : hintHashes) {
            crackedHints.put(hintHash, null);
        }

        bruteForcePermutations(crackedHints);

        return crackedHints;
    }
}
