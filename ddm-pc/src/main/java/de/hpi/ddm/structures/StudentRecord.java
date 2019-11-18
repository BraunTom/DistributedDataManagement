package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @NoArgsConstructor @AllArgsConstructor
public class StudentRecord implements Serializable {
    private static final long serialVersionUID = 3478429357206727140L;

    int id;
    String name;

    String passwordChars;
    int passwordLength;

    SHA256Hash fullPasswordHash;
    SHA256Hash[] hintHashes;
}
