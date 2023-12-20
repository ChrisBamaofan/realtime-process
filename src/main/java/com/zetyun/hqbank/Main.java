package com.zetyun.hqbank;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.dds.DDSData;

import java.io.IOException;

/**
 * @author zhaohaojie
 * @date 2023-12-19 16:29
 */
public class Main {
    public static void main(String[] args) {
        String a = "{\"scn\":82003135,\"tms\":\"2023-12-12 15:01:29\",\"xid\":\"2.4.30661\",\"payload\":{\"op\":\"d\",\"schema\":{\"owner\":\"DDS\",\"table\":\"T01\"},\"row\":1,\"rid\":\"AAAVwgAAEAAACQPAAO\",\"before\":{\"C1\":24,\"C2\":\"d25\",\"C3\":\"2023-12-12 10:35:09\"}}}";
        String b= "{\"payload\":{\"op\":\"c\",\"schema\":{\"owner\":\"DDS\",\"table\":\"T01\"},\"after\":{\"C1\":1,\"C2\":\"aaaaaaaa\",\"C3\":1696745779000}},\"row\":55}";
        String c = "{\"scn\":82002751,\"tms\":\"2023-12-12 14:57:50\",\"xid\":\"10.15.408343\",\"payload\":{\"op\":\"u\",\"schema\":{\"owner\":\"DDS\",\"table\":\"T01\"},\"row\":2,\"rid\":\"AAAVwgAAEAAACQPAAP\",\"before\":{\"C1\":24,\"C2\":\"d24\",\"C3\":\"2023-12-12 13:49:23\"},\"after\":{\"C1\":24,\"C2\":\"d25\",\"C3\":\"2023-12-12 13:49:23\"}}}";

        ObjectMapper om = new ObjectMapper();
        try {
            DDSData ddsPayload = om.readValue(a, DDSData.class);
            DDSData bPady = om.readValue(b, DDSData.class);
            DDSData cPady = om.readValue(c, DDSData.class);
            System.out.println(ddsPayload.getPayload().getOp());
            System.out.println(bPady.getPayload().getOp());
            System.out.println(cPady.getPayload().getOp());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

