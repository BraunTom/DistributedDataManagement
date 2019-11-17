package de.hpi.ddm.structures;

import lombok.Value;

import java.io.Serializable;

@Value
public class StudentRecord implements Serializable {
    private static final long serialVersionUID = 3478429357206727140L;

    int id;
    String name;

    String passwordChars;
    int passwordLength;

    SHA256Hash fullPasswordHash;
    SHA256Hash[] hintHashes;
}
