package de.hpi.ddm.algorithms;

import de.hpi.ddm.structures.SHA256Hash;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HintPermutationCrackerTest {
    @Test
    public void crack_whenCrackingMultipleMatchingHintHashes_itShouldCrackThemSuccessfully() {
        // Arrange
        SHA256Hash h1 = SHA256Hash.fromHexString("e9c0f8b575cbfcb42ab3b78ecc87efa3b011d9a5d10b09fa4e96f240bf6a82f5"); // ABCDEF
        SHA256Hash h2 = SHA256Hash.fromHexString("dbd17ff94e777edbd8114bb355a74c3a07571ad4dc92d66d41521df76efd14cc"); // ACDEGB
        SHA256Hash h3 = SHA256Hash.fromHexString("c03301c60848edf9474c610f528d9b30403ce35a9f227486bd4050d63b1ad5e0"); // DEFCGA
        SHA256Hash[] hintHashes = new SHA256Hash[] { h1, h2, h3 };

        HintPermutationCracker cracker = new HintPermutationCracker("ABCDEFG");

        // Act
        Map<SHA256Hash, HintPermutationCracker.CrackedHint> crackedHints = cracker.crack(hintHashes);

        // Assert
        assertEquals(3, crackedHints.size());
        assertTrue(crackedHints.containsKey(h1));
        assertTrue(crackedHints.containsKey(h2));
        assertTrue(crackedHints.containsKey(h3));
        assertEquals("ABCDEF", crackedHints.get(h1).getPlainText());
        assertEquals('G', crackedHints.get(h1).getMissingCharacter());
        assertEquals("ACDEGB", crackedHints.get(h2).getPlainText());
        assertEquals('F', crackedHints.get(h2).getMissingCharacter());
        assertEquals("DEFCGA", crackedHints.get(h3).getPlainText());
        assertEquals('B', crackedHints.get(h3).getMissingCharacter());
    }

    @Test(expected = RuntimeException.class)
    public void crack_whenCrackingMultipleHintHashesWithANonMatchingHash_itShouldCrackThemSuccessfully() {
        // Arrange
        SHA256Hash h1 = SHA256Hash.fromHexString("e9c0f8b575cbfcb42ab3b78ecc87efa3b011d9a5d10b09fa4e96f240bf6a82f5"); // ABCDEF
        SHA256Hash h2 = SHA256Hash.fromHexString("0123456789012345678901234567890123456789012345678901234567890123"); // (Dummy non-matching)
        SHA256Hash[] hintHashes = new SHA256Hash[] { h1, h2 };

        HintPermutationCracker cracker = new HintPermutationCracker("ABCDEFG");

        // Act
        cracker.crack(hintHashes);
    }
}