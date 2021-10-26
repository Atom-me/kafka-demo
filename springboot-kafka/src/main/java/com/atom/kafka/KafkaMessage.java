package com.atom.kafka;

import lombok.Data;

import java.util.Date;

/**
 * @author Atom
 */
@Data
public class KafkaMessage {

    private Long id;
    private String username;
    private String password;
    private Date date;

}
