package com.sdb.transfer;

import com.sdb.auth.sequoia.SequoiaDBConn;
import com.sdb.config.ConfigLoader;
import com.sdb.stat.Total;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @ClassName SequoiaDBReader
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 14:00
 * @Version 1.0
 **/
public class SequoiaDBReader implements DBReader,Runnable{
    private final static Logger log = LoggerFactory.getLogger(SequoiaDBReader.class);


    private String cs;
    private String cl;
    private Sequoiadb sequoiadb;
    private long totalNum;
    private static long preNum = 0;
    private static long preMs = System.currentTimeMillis();
    private List<BlockingQueue<String>> recs;
    private DBCollection collection;
    private static String collectionName;

    public static void setCollectionName(String collectionName) {
        SequoiaDBReader.collectionName = collectionName;
    }

    public void setTotalNum(long totalNum) {
        this.totalNum = totalNum;
    }

    public SequoiaDBReader(String cs, String cl, long totalNum, List<BlockingQueue<String>> recs) {
        this.cs = cs;
        this.cl = cl;
        this.recs = recs;
        collectionName = cl;
        try {
            this.sequoiadb = SequoiaDBConn.getConnection();
            this.totalNum = totalNum;
        } catch (Exception e) {
            log.error("there is an exception when connect to sequoiaDB, error message: {}",e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            checkClRecNum();
        }catch (Exception e){
            log.debug("统计任务执行出错，错误信息：{}",e.getMessage());
        }

    }


    public long getClCount(String cs,String cl){
        return sequoiadb.getCollectionSpace(cs).getCollection(cl).getCount();
    }
    private void checkClRecNum(){
        if (collectionName == null){
            return;
        }
        if (!cl.equals(collectionName)){
            this.cl = collectionName;
            preNum = 0;
            preMs = System.currentTimeMillis();
        }
        long curMs = System.currentTimeMillis();
        long count = Total.TOTAL_SUCCESS_NUM.get();
        int totalRead = Total.TOTAL_READ_NUM.get();
        int failed = Total.TOTAL_FAILED_NUM.get();
        double speed = (count-preNum)/ ((curMs-preMs)/1000);
        log.info("collection:{}.{},total record:{},success record:{},failed record:{},speed:{}/s,total read:{}",
                this.cs,collectionName,totalNum,count,failed,speed,totalRead);
        preNum = count;
        preMs = curMs;
    }
}
