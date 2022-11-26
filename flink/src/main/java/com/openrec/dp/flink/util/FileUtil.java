package com.openrec.dp.flink.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;


@Slf4j
public class FileUtil {

    public static Properties loadProperties(String fileName) {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        Properties properties = null;
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
            properties = new Properties();
            properties.load(inputStreamReader);
        } catch (IOException e) {
            log.error("load properties failed: {}", ExceptionUtils.getStackTrace(e));
        }
        return properties;
    }
}
