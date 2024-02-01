package com.zetyun.hqbank.util;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * @author zhaohaojie
 * @date 2024-02-01 14:20
 */
public class KafkaUtil {

    //        orcl-dds-t_zhj2
    // target-orcl-dds-t_zhj2
    public static List<String> getKafkaTopic(String dbname, String owner, List<String> tables){

        List<String> result = new ArrayList<>();
        if (CollectionUtils.isEmpty(tables)){
            return result;
        }
        for (String table:tables){
            String topic = dbname.toLowerCase(Locale.ROOT)+"-"+owner.toLowerCase(Locale.ROOT)+"-"+table.toLowerCase(Locale.ROOT);
            result.add(topic);
        }
        return result;
    }

    public static List<String> getKafkaTopicB(String dbname, String owner, List<String> tables,String prefix){

        List<String> result = new ArrayList<>();
        if (CollectionUtils.isEmpty(tables)){
            return result;
        }
        for (String table:tables){
            String topic = prefix.toLowerCase(Locale.ROOT)+"-"+dbname.toLowerCase(Locale.ROOT)+"-"+owner.toLowerCase(Locale.ROOT)+"-"+table.toLowerCase(Locale.ROOT);
            result.add(topic);
        }
        return result;
    }
}

