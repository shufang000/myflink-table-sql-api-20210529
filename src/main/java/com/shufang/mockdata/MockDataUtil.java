package com.shufang.mockdata;

import java.util.Random;

/**
 * 该类主要用来模拟生成用户操作的数据
 */
public class MockDataUtil {

    public static void main(String[] args) throws InterruptedException {
        Random random = new Random();
        String peoples = "Bob,Kelly,Micheal,Milly,Ben,Justin,Tyler,Rae";
        String actions = "点击,收藏,取消收藏,加购,下单,付款,点赞";

        String[] peopleList = peoples.split(",");
        String[] actionList = actions.split(",");

        for (int i = 0; i < 30; i++) {

            String user_name = peopleList[random.nextInt(peopleList.length)];
            String user_action = actionList[random.nextInt(actionList.length)];
            long timestamp = System.currentTimeMillis();
            String record = new StringBuilder(user_name).append(",").append(user_action).append(",").append(timestamp).toString();
            System.out.println(record);
            Thread.sleep(1000);
        }

    }

}
