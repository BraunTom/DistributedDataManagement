package de.hpi.ddm.algorithms;

import de.hpi.ddm.structures.SHA256Hash;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class HintPermutationCrackerTest {
    @Test
    public void crack_whenCrackingMultipleMatchingHintHashes_itShouldCrackThemSuccessfully() {
        // Arrange
        SHA256Hash h1 = SHA256Hash.fromHexString("e9c0f8b575cbfcb42ab3b78ecc87efa3b011d9a5d10b09fa4e96f240bf6a82f5"); // ABCDEF
        SHA256Hash h2 = SHA256Hash.fromHexString("dbd17ff94e777edbd8114bb355a74c3a07571ad4dc92d66d41521df76efd14cc"); // ACDEGB
        SHA256Hash h3 = SHA256Hash.fromHexString("c03301c60848edf9474c610f528d9b30403ce35a9f227486bd4050d63b1ad5e0"); // DEFCGA
        SHA256Hash[] hintHashes = new SHA256Hash[] { h1, h2, h3 };

        HintPermutationCracker cracker = new HintPermutationCracker("ABCDEFG", null);

        // Act
        Map<SHA256Hash, HintPermutationCracker.CrackedHint> crackedHints = cracker.crack(hintHashes);

        // Assert
        assertEquals(3, crackedHints.size());
        assertTrue(crackedHints.containsKey(h1));
        assertTrue(crackedHints.containsKey(h2));
        assertTrue(crackedHints.containsKey(h3));
        assertNotEquals(null, crackedHints.get(h1));
        assertEquals("ABCDEF", crackedHints.get(h1).getPlainText());
        assertEquals('G', crackedHints.get(h1).getMissingCharacter());
        assertNotEquals(null, crackedHints.get(h2));
        assertEquals("ACDEGB", crackedHints.get(h2).getPlainText());
        assertEquals('F', crackedHints.get(h2).getMissingCharacter());
        assertNotEquals(null, crackedHints.get(h3));
        assertEquals("DEFCGA", crackedHints.get(h3).getPlainText());
        assertEquals('B', crackedHints.get(h3).getMissingCharacter());
    }

    @Test
    public void crack_whenCrackingMultipleHintHashesWithANonMatchingHash_itShouldOnlyReturnTheCrackedOnes() {
        // Arrange
        SHA256Hash h1 = SHA256Hash.fromHexString("e9c0f8b575cbfcb42ab3b78ecc87efa3b011d9a5d10b09fa4e96f240bf6a82f5"); // ABCDEF
        SHA256Hash h2 = SHA256Hash.fromHexString("0123456789012345678901234567890123456789012345678901234567890123"); // (Dummy non-matching)
        SHA256Hash[] hintHashes = new SHA256Hash[] { h1, h2 };

        HintPermutationCracker cracker = new HintPermutationCracker("ABCDEFG", null);

        // Act
        Map<SHA256Hash, HintPermutationCracker.CrackedHint> crackedHints = cracker.crack(hintHashes);

        // Assert
        assertEquals(2, crackedHints.size());
        assertTrue(crackedHints.containsKey(h1));
        assertTrue(crackedHints.containsKey(h2));
        assertNotEquals(crackedHints.get(h1), null);
        assertEquals("ABCDEF", crackedHints.get(h1).getPlainText());
        assertEquals('G', crackedHints.get(h1).getMissingCharacter());
        assertEquals(crackedHints.get(h2), null);
    }

    @Test
    public void crack_whenCrackingAPasswordWithAPrefix_itShouldUseThePrefix() {
        // Arrange
        SHA256Hash h1 = SHA256Hash.fromHexString("d590aa3f2b1e37ef59db75670359bb68bec79d4938901042095bfa4032d64b25"); // TheQuickBrownFoxJumpedOverTheLazyDogACDB
        SHA256Hash[] hintHashes = new SHA256Hash[] { h1 };

        HintPermutationCracker cracker = new HintPermutationCracker("ABCDE", "TheQuickBrownFoxJumpedOverTheLazyDog");

        // Act
        Map<SHA256Hash, HintPermutationCracker.CrackedHint> crackedHints = cracker.crack(hintHashes);

        // Assert
        assertEquals(1, crackedHints.size());
        assertTrue(crackedHints.containsKey(h1));
        assertNotEquals(crackedHints.get(h1), null);
        assertEquals("TheQuickBrownFoxJumpedOverTheLazyDogACDB", crackedHints.get(h1).getPlainText());
        assertEquals('E', crackedHints.get(h1).getMissingCharacter());
    }
}