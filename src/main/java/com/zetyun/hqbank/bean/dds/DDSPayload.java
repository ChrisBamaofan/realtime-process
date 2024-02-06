package com.zetyun.hqbank.bean.dds;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;


import java.security.MessageDigest;
import java.util.Formatter;
import java.util.HashMap;

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
    String sql;

    public DDSPayload createDummy(String schema){
        this.setOp("c");
        DDSSchema schema1 = new DDSSchema();
        schema1.setOwner(schema);
        schema1.setTable("ICE_DEMO");
        this.setSchema(schema1);
        this.setRow(1);
        this.setRid("AAAWAgAAEAAACRmAAB");
        HashMap<String,Object> object = new HashMap();
        object.put("C1",1);
        this.setAfter(object);
        return this;
    }

    public static void main(String[] args) {
        ObjectMapper om = new ObjectMapper();
        String a = "{\"CHAR1\":\"y\",\"CHAR2\":\"y\",\"CHAR3\":\"char3\",\"DATE_1\":null,\"CHAR4\":null,\"LONG1\":null,\"NUM1\":null,\"NUM2\":null,\"INT1\":null,\"FLOAT1\":null,\"REAL1\":null}";
        String b = "{\"CHAR1\":\"n\",\"CHAR2\":\"y\",\"CHAR3\":\"char2\",\"DATE_1\":\"2022-11-22 00:00:00\",\"CHAR4\":null,\"LONG1\":null,\"NUM1\":null,\"NUM2\":null,\"INT1\":null,\"FLOAT1\":null,\"REAL1\":null}";
        try {
            HashMap<String,Object> map = om.readValue(a,HashMap.class);
            String aa = encrypt(a);

            String bb = encrypt(b);
            System.out.println(aa);
            System.out.println(bb);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final String MD5_ALGORITHM = "MD5";
    public static String encrypt(String data) throws Exception {
        // 获取MD5算法实例
        MessageDigest messageDigest = MessageDigest.getInstance(MD5_ALGORITHM);
        // 计算散列值
        byte[] digest = messageDigest.digest(data.getBytes());
        Formatter formatter = new Formatter();
        // 补齐前导0，并格式化
        for (byte b : digest) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}

