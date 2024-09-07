package com.zetyun.hqbank.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.Riches;
import com.zetyun.hqbank.bean.Student;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author zhaohaojie
 * @date 2024-08-23 11:56
 */
public class RichesDeserializationSchema implements DeserializationSchema<Riches> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Riches deserialize(byte[] message) throws IOException {
        System.out.println(" richesDeserialization deserialize method ");
        return objectMapper.readValue(message, Riches.class);
    }

    @Override
    public boolean isEndOfStream(Riches nextElement) {
        System.out.println(" richesDeserialization isEndOfStream method ");
        return false;
    }

    @Override
    public TypeInformation<Riches> getProducedType() {
        System.out.println(" richesDeserialization getProducedType method ");
        return TypeInformation.of(Riches.class);
    }
}

