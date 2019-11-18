package de.hpi.ddm.algorithms;

import de.hpi.ddm.structures.SHA256Hash;
import org.junit.Test;

import java.security.DigestException;

import static org.junit.Assert.assertEquals;

public class CombinationCrackerTest {

    @Test
    public void crack_whenTryingALongPasswordWithFewChoices_itShouldSuccessfullyCrackIt() throws DigestException {
        // Arrange
        CombinationCracker cracker = new CombinationCracker("AB", 8);
        SHA256Hash targetHash = SHA256Hash.fromHexString("06e39dc6170e54239d73836b0574a2482720539f91ed8ca308b9e3a1a51225d2");

        // Act
        String plainText = cracker.crack(targetHash);

        // Assert
        assertEquals("ABABABAB", plainText);
    }

    @Test
    public void crack_whenTryingAShortPasswordWithManyChoices_itShouldSuccessfullyCrackIt() throws DigestException {
        // Arrange
        CombinationCracker cracker = new CombinationCracker("ABCDabcd", 6);
        SHA256Hash targetHash = SHA256Hash.fromHexString("9eda4e3054486d88bf3440717f8b304cddfd4f1e45d6feee1b9be4c5b3ff40cd");

        // Act
        String plainText = cracker.crack(targetHash);

        // Assert
        assertEquals("AbBaCd", plainText);
    }

    @Test(expected = RuntimeException.class)
    public void crack_whenNoCombinationMatchesTheGivenHash_itShouldThrowAnException() throws DigestException {
        // Arrange
        CombinationCracker cracker = new CombinationCracker("AB", 4);
        SHA256Hash targetHash = SHA256Hash.fromHexString("0123456789012345678901234567890123456789012345678901234567890123");

        // Act
        cracker.crack(targetHash);
    }
}