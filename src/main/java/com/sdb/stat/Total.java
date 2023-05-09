package com.sdb.stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName Total
 * @Description 统计信息，全局原子变量
 * @Author yangqi
 * @Date 2023/3/16 05:15
 * @Version 1.0
 **/
public class Total {
    public static AtomicInteger TOTAL_SUCCESS_NUM = new AtomicInteger(0);
    public static AtomicInteger TOTAL_FAILED_NUM = new AtomicInteger(0);
    public static AtomicInteger TOTAL_READ_NUM =  new AtomicInteger(0);
    public static CountDownLatch COUNT_DOWN_LATCH;
    public static List<String> ALL_STATS = new ArrayList<>();


    public static void registerTask(String sourceDb,String sourceTb,String targetDb,String targetTb,long total,long success,long failed){
        String result = String.format("source table:%s.%s\t,target table:%s.%s\t,total record:%d\t,total success:%d\t,total failed:%d\t",sourceDb,sourceTb,targetDb,targetTb,total,success,failed);
        ALL_STATS.add(result);
    }

    public static void reset(){
        TOTAL_READ_NUM.set(0);
        TOTAL_FAILED_NUM.set(0);
        TOTAL_SUCCESS_NUM.set(0);
    }

    public static boolean isEnd(long total){
        long dealNum = TOTAL_FAILED_NUM.get() + TOTAL_SUCCESS_NUM.get();
        return dealNum == total;
    }
}
