package com.shufang.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MyUtil {

    static Properties properties = new Properties();

    static {
        InputStream in = MyUtil.class.getClassLoader().getResourceAsStream("jdbc.properties");
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取一个文件的路径
     * @param key
     * @return
     */
    public static String getPath(String key) {
        return properties.getProperty(key);
    }


}


