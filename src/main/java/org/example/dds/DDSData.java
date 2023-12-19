package org.example.dds;

import lombok.Data;

/**
 * @author zhaohaojie
 * @date 2023-12-19 15:56
 */
@Data
public class DDSData {
    long scn;
    String tms;
    String xid;
    String row;
    DDSPayload payload;
}

