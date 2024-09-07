package com.zetyun.hqbank.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.ClickEvents;
import com.zetyun.hqbank.bean.Riches;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author zhaohaojie
 * @date 2024-08-23 11:56
 */
public class ClickEventsDeserializationSchema implements DeserializationSchema<ClickEvents> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ClickEvents deserialize(byte[] message) throws IOException {
        System.out.println(" richesDeserialization deserialize method ");
        return objectMapper.readValue(message, ClickEvents.class);
    }

    @Override
    public boolean isEndOfStream(ClickEvents nextElement) {
        System.out.println(" richesDeserialization isEndOfStream method ");
        return false;
    }

    @Override
    public TypeInformation<ClickEvents> getProducedType() {
        System.out.println(" richesDeserialization getProducedType method ");
        return TypeInformation.of(ClickEvents.class);
    }
}

