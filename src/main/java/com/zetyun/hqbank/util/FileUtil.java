package com.zetyun.hqbank.util;

import org.springframework.core.io.ClassPathResource;
import org.springframework.util.FileCopyUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * @author zhaohaojie
 * @date 2023-12-20 17:18
 */
public class FileUtil {

    public static String readFile(String fileName){
        StringBuilder fileContent = new StringBuilder();
        try (InputStream inputStream = FileUtil.class.getClassLoader().getResourceAsStream("ddl/"+fileName)) {
            if (inputStream != null) {
                // 使用 Scanner 读取文件内容
                try (Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name())) {
                    while (scanner.hasNextLine()) {
                        fileContent.append(scanner.nextLine()).append(System.lineSeparator());
                    }

                    // 打印文件内容
                    System.out.println("File Content:\n" + fileContent.toString());

                }
            } else {
                System.err.println("File not found!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileContent.toString();
    }

}

