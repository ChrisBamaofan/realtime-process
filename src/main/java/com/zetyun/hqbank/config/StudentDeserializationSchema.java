package com.zetyun.hqbank.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetyun.hqbank.bean.Student;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author zhaohaojie
 * @date 2024-08-23 11:56
 */
public class StudentDeserializationSchema  implements DeserializationSchema<Student> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Student deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Student.class);
    }

    @Override
    public boolean isEndOfStream(Student nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Student> getProducedType() {
        return TypeInformation.of(Student.class);
    }
}

