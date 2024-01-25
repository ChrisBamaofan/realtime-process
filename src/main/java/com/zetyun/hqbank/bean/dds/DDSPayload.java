package com.zetyun.hqbank.bean.dds;

import lombok.Data;
import org.json.JSONObject;

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
}

