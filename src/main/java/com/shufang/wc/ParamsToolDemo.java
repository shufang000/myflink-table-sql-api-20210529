package com.shufang.wc;

import org.apache.flink.api.java.utils.ParameterTool;

public class ParamsToolDemo {
    public static void main(String[] args) {

        System.out.println(args.length); //4
        ParameterTool tool = ParameterTool.fromArgs(args);

        String host = tool.get("host");
        int port = tool.getInt("port");

        System.out.printf("host = %s",host);
        System.out.println();
        System.out.printf("port = %d",port);



    }
}
