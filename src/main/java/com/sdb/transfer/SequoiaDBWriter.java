package com.sdb.transfer;

import com.sdb.auth.sequoia.SequoiaDBConn;
import com.sdb.stat.Total;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.exception.SDBError;
import org.sequoiadb.bson.BSONObject;
import org.sequoiadb.bson.BasicBSONObject;
import org.sequoiadb.bson.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName SequoiaDBWriter
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 14:00
 * @Version 1.0
 **/
public class SequoiaDBWriter implements DBWriter,Runnable{

    private final static Logger log = LoggerFactory.getLogger(SequoiaDBWriter.class);

    private String cs;
    private String cl;
    private Sequoiadb sequoiadb;
    private BlockingQueue<String> recs;
    private long num;
    private int writeBatch;

    public SequoiaDBWriter(String cs, String cl, BlockingQueue<String> recs, long num,int writeBatch) {
        this.cs = cs;
        this.cl = cl;
        this.recs = recs;
        this.num = num;
        this.writeBatch = writeBatch;
        try {
            this.sequoiadb = SequoiaDBConn.getConnection();
        } catch (InterruptedException e) {
            log.error("there is an exception when connect to sequoiaDB, error message: {}",e.getMessage());
        }
    }

    @Override
    public void run() {
        log.debug("my sequoaidb write task is start .count down latch:{}",Total.COUNT_DOWN_LATCH.getCount());
        if (num > 0){
            writeToSdb();
        }
        SequoiaDBConn.releaseConnection(sequoiadb);
        Total.COUNT_DOWN_LATCH.countDown();
        log.debug("my sequoaidb write task is completed , ready to exit. count down latch:{}",Total.COUNT_DOWN_LATCH.getCount());
    }

    public void writeToSdb(){
        DBCollection collection = sequoiadb.getCollectionSpace(cs).getCollection(cl);
        List<BSONObject> batch = new ArrayList<>();

        int writeNum = 0;
        //while (writeNum < num){
        while (!Total.isEnd(this.num)){
            for (int i = 0;i<writeBatch;i++){
                String poll = null;
                try {
                    poll = recs.poll();
                } catch (Exception e) {
                    log.error("there is an exception when poll record from queue,error msg:{}",e.getMessage());
                }
                if ( poll !=null ){
                    log.debug("<========== poll record from queue success:{}",poll);
                    try {
                        BasicBSONObject parse =(BasicBSONObject) JSON.parse(poll);
                        batch.add(parse);
                    }catch (Exception e){
                        log.error("string to json failed.failed record:{},error message:{}.",poll,e.getMessage());
                    }
                }
            }
            if (batch.size() == 0){
                continue;
            }
            try {
                log.debug("<========== begin transaction...");
                sequoiadb.beginTransaction();
                collection.insert(batch);
                sequoiadb.commit();
                log.debug("<========== commit transaction...");
                log.debug("<========== insert records to SequoiaDB success:{},total insert:{}",batch.size(),num);
                Total.TOTAL_SUCCESS_NUM.addAndGet(batch.size());
            }catch (Exception e){
                log.error("bulk insert into sdb failed.message:{},try to insert one by one to sequoiadb",
                        e.getMessage());
                if (checkConn()){
                    sequoiadb.rollback();
                    log.debug("<========== rollback transaction...");
                }else {
                    resetConn();
                }
                dealFailed(batch,collection);
                if (((BaseException)e).getErrorCode() != SDBError.SDB_IXM_DUP_KEY.getErrorCode()){
                    break;
                }
            }
            writeNum += batch.size();
            batch.clear();
        }
    }

    private boolean checkConn(){
        try {
            if (this.sequoiadb.isValid()){
                log.info("check sequoiadb connection success,this connection is normal,my thread name:{}",Thread.currentThread().getName());
                return true;
            }
        }catch (Exception exception){
            log.error("check sequoiadb connection failed,error message:{}",exception.getMessage());
        }
        return false;
    }

    private void resetConn(){
        try {
            sequoiadb.close();
            SequoiaDBConn.resetConn(this.sequoiadb);
            this.sequoiadb = SequoiaDBConn.getConnection();
        }catch (Exception e){
            e.printStackTrace();
            log.error("get sequoiadb connection failed ,error msg:{}",e.getMessage());
        }
    }


    private void dealOne(BSONObject r,DBCollection collection){
        try {
            collection.insert(r);
            log.debug("<=========write record to sequoiadb success. record content:{}",r.toString());
            Total.TOTAL_SUCCESS_NUM.addAndGet(1);
        }catch (BaseException e){
            if (e.getErrorCode() == SDBError.SDB_IXM_DUP_KEY.getErrorCode()){
                log.error("insert into sdb failed. error message:{},failed record:{}",
                        e.getMessage(),r.get("_id"));
                Total.TOTAL_FAILED_NUM.addAndGet(1);
            }else {
                boolean result = false;
                while (!result){
                    result = recs.offer(r.toString());
                }
                log.debug("=========>insert one record to sequoiadb failed,turn back to queue,record content:{}",r);
            }
        }
    }

    private void dealFailed(List<BSONObject> list,DBCollection collection){
        for (BSONObject r : list) {
            dealOne(r,collection);
        }
    }

}