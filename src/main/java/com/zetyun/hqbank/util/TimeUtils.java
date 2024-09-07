package com.zetyun.hqbank.util;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhaohaojie
 * @date 2020-12-12 22:21
 */
public class TimeUtils {
    public static List<DayOfWeek> workDays = Arrays.asList(DayOfWeek.MONDAY,DayOfWeek.TUESDAY,DayOfWeek.WEDNESDAY,DayOfWeek.THURSDAY,DayOfWeek.FRIDAY);
    public static final String FORMAT1 = "yyyyMMddHHmmss";
    public static LocalDateTime timestamToDatetime(long timestamp){
        Instant instant = Instant.ofEpochMilli(timestamp);
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    public static long datatimeToTimestamp(LocalDateTime ldt){
        long timestamp = ldt.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return timestamp;
    }




    public static void main(String[] args) {

        LocalDateTime tiem = timestamToDatetime(694108800000L);
        DateTimeFormatter formatter =  DateTimeFormatter.ofPattern( FORMAT1);
        String format = tiem.format(formatter);
        System.out.println("aa"+format);
        datatimeToTimestamp(tiem);

    }
}

