package org.example.dds;

import lombok.Data;

/**
 * @author zhaohaojie
 * @date 2023-12-19 15:58
 */
@Data
public class DDSPayload {
    String op;
    DDSSchema schema;
    int row;
    String rid;
    Object before;
    Object after;
}

