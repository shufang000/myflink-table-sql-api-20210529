package com.shufang.first;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("hello world");
        System.out.println(System.currentTimeMillis());


        Random random = new Random();

//        while (true){
//            System.out.println(random.nextInt(5));
//        }


        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String format = simpleDateFormat.format(new Date(1622642535579L));

        System.out.println(format);

    }

}
