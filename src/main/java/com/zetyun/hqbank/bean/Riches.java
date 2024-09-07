package com.zetyun.hqbank.bean;

import lombok.Data;

/**
 * @author zhaohaojie
 * @date 2024-09-01 22:44
 */
@Data
public class Riches {
    /**
     * user id
     */
    Integer id;
    /**
     * user finance
     */
    Integer riches;
    /**
     * timestamp
     */
    Long ts;
    /**
     * 财富来源地
     */
//    String from;
}

