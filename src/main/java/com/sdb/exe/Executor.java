package com.sdb.exe;

import com.sdb.auth.mongo.MongoDBConn;
import com.sdb.auth.sequoia.SequoiaDBConn;
import com.sdb.config.ConfigInfo;
import com.sdb.stat.Total;
import com.sdb.transfer.MongoDBReader;
import com.sdb.transfer.Prepare;
import com.sdb.transfer.SequoiaDBReader;
import com.sdb.transfer.SequoiaDBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @ClassName Executor
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 17:18
 * @Version 1.0
 **/
public class Executor {
    private final static Logger log = LoggerFactory.getLogger(Executor.class);
    private ExecutorService readPool;
    private ExecutorService writePool;
    private List<BlockingQueue<String>> recs;
    private ScheduledExecutorService scheduledExecutorService;
    private ConfigInfo configInfo;
    private List<String> sourceTbs;
    private List<String> targetTbs;


    public Executor(ConfigInfo configInfo,List<String> sourceTbs,List<String> targetTbs) {
        this.configInfo = configInfo;

        this.sourceTbs = sourceTbs;
        this.targetTbs = targetTbs;
    }

    public Executor init(){
        readPool = Executors.newFixedThreadPool(configInfo.getReadThreads());
        writePool = Executors.newFixedThreadPool(configInfo.getWriteThreads());
        recs = new ArrayList<>(configInfo.getReadThreads());
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        return this;
    }

    public void start() throws InterruptedException {

        //检测源表数量与目标表数量的合法性
        if (sourceTbs.size() != targetTbs.size()){
            log.error("source table num:{},target table num:{},not equals,quit.",sourceTbs.size(),targetTbs.size());
            throw new  RuntimeException("source table not same to target table.");
        }

        int speedDelay = configInfo.getSpeedDelay();
        SequoiaDBReader monitor = new SequoiaDBReader(configInfo.getTargetDb(), "", 0, recs);
        for (int i = 0;i<sourceTbs.size();i++){
            log.info("================start {}.{} transfer to {}.{}===================",
                    configInfo.getSourceDb(),sourceTbs.get(i),
                    configInfo.getTargetDb(),targetTbs.get(i));
            if (!Prepare.checkMongo(MongoDBConn.getConn(),configInfo.getSourceDb(),sourceTbs.get(i))){
                log.info("==================end {}.{} transfer to {}.{}===================",
                        configInfo.getSourceDb(),sourceTbs.get(i),
                        configInfo.getTargetDb(),targetTbs.get(i));
                Total.registerTask(configInfo.getSourceDb(),sourceTbs.get(i),
                        configInfo.getTargetDb(),targetTbs.get(i),
                        Prepare.getTotalNum(configInfo.getSourceDb(),sourceTbs.get(i)),
                        Total.TOTAL_SUCCESS_NUM.get(),Total.TOTAL_FAILED_NUM.get());
                continue;
            }
            if (!Prepare.checkSdb(SequoiaDBConn.getConnection(),configInfo.getTargetDb(),targetTbs.get(i))){
                log.info("==================failed {}.{} transfer to {}.{}===================",
                        configInfo.getSourceDb(),sourceTbs.get(i),
                        configInfo.getTargetDb(),targetTbs.get(i));
                Total.registerTask(configInfo.getSourceDb(),sourceTbs.get(i),
                        configInfo.getTargetDb(),targetTbs.get(i),
                        Prepare.getTotalNum(configInfo.getSourceDb(),sourceTbs.get(i)),
                        Total.TOTAL_SUCCESS_NUM.get(),Total.TOTAL_FAILED_NUM.get());
                continue;
            }

            //开启统计任务
            scheduledExecutorService.scheduleAtFixedRate(monitor,speedDelay,speedDelay,TimeUnit.SECONDS);
            MongoDBReader.setLastId(null);
            SequoiaDBReader.setCollectionName(targetTbs.get(i));
            long num = Prepare.getTotalNum(configInfo.getSourceDb(),sourceTbs.get(i));
            //创建定时器
            Total.COUNT_DOWN_LATCH = new CountDownLatch(configInfo.getReadThreads()+configInfo.getWriteThreads());
            monitor.setTotalNum(num);
            //开启迁移任务
            start(configInfo.getSourceDb(),sourceTbs.get(i),configInfo.getTargetDb(),targetTbs.get(i),num);
            //等待迁移线程结束
            Total.COUNT_DOWN_LATCH.await();
            //注册迁移任务
            Total.registerTask(configInfo.getSourceDb(),sourceTbs.get(i),
                    configInfo.getTargetDb(),targetTbs.get(i),
                    Prepare.getTotalNum(configInfo.getSourceDb(),sourceTbs.get(i)),
                    Total.TOTAL_SUCCESS_NUM.get(),Total.TOTAL_FAILED_NUM.get());
            Thread.sleep(configInfo.getSpeedDelay()*1000);
            //重置全局统计信息
            Total.reset();
            log.info("==================end {}.{} transfer to {}.{}===================",
                    configInfo.getSourceDb(),sourceTbs.get(i),
                    configInfo.getTargetDb(),targetTbs.get(i));


        }
        scheduledExecutorService.shutdownNow();
    }

    public void start(String sourceDb,String sourceTb,String targetDb,String targetTb,long totalNum){
        int readNum = configInfo.getReadThreads();
        int writeNum = configInfo.getWriteThreads();
        int writeBatchSize = configInfo.getWriteBatchSize();
        int readBatchSize = configInfo.getReadBatchSize();
        int blockThreadRadio = configInfo.getBlockThreadRadio();
        //获取每个读写线程需要处理的记录数
        long readRecs = totalNum/readNum;
        long extraRecs = totalNum%readNum;
        //long readRecs = totalNum/(readNum-1);
        long writeRecs = totalNum/writeNum + (totalNum%writeNum !=0 ?1:0);
        int skip = 0;
        for (int i = 0;i<readNum;i++){
            if (recs.size() != readNum){
                BlockingQueue<String> rec = new ArrayBlockingQueue<>(configInfo.getReadThreads()*blockThreadRadio);
                recs.add(rec);
            }

            if (i<extraRecs){
                readPool.submit(new MongoDBReader(sourceDb,sourceTb,readRecs+1,skip,recs.get(0),readBatchSize));
                skip += readRecs+1;
            }else {
                readPool.submit(new MongoDBReader(sourceDb,sourceTb,readRecs,skip,recs.get(0),readBatchSize));
                skip += readRecs;
            }
        }
        log.debug("{} read threads start success.",readNum);
        //int writeTmp = 0;
        /*
        for (int i = 0;i<writeNum;i++){
            long curWrite = writeRecs;
            if (totalNum - writeTmp < writeRecs){
                curWrite = totalNum-writeTmp;
            }
            log.debug("my task need to deal records num is :{}",curWrite);
            writePool.submit(new SequoiaDBWriter(targetDb,targetTb,recs.get(0),curWrite,writeBatchSize));
            writeTmp += curWrite;
        }

         */
        for (int i = 0;i<writeNum;i++){
            writePool.submit(new SequoiaDBWriter(targetDb,targetTb,recs.get(0),totalNum,writeBatchSize));
        }
        log.debug("{} write threads start success.",writeNum);
    }
}
