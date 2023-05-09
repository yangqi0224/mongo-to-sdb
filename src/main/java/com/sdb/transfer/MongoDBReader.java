package com.sdb.transfer;

import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.sdb.auth.mongo.MongoDBConn;
import com.sdb.stat.Total;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Filter;

/**
 * @ClassName MongoDBReader
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 14:00
 * @Version 1.0
 **/
public class MongoDBReader implements DBReader,Runnable{

    private final static Logger log = LoggerFactory.getLogger(MongoDBReader.class);

    private static ObjectId LAST_ID = null;
    private String db;
    private String tb;
    private MongoClient client;
    private long limit;
    private long skip;
    private BlockingQueue<String> recQueue;
    private int readBatch;

    public MongoDBReader(String db, String tb, long limit, long skip, BlockingQueue<String> recQueue,int readBatch) {
        this.db = db;
        this.tb = tb;
        this.limit = limit;
        this.skip = skip;
        this.recQueue = recQueue;
        this.readBatch = readBatch;
        this.client = MongoDBConn.getConn();
    }

    public static void setLastId(ObjectId lastId) {
        LAST_ID = lastId;
    }

    public MongoDBReader() {
        this.client = MongoDBConn.getConn();
    }

    @Override
    public void run() {
        log.debug("my mongodb read task is start.");
        if (limit > 0){
            readRecs();
        }
        MongoDBConn.releaseConn(client);
        Total.COUNT_DOWN_LATCH.countDown();
        log.debug("my mongodb read task is completed , ready to exit. count down latch:{}",Total.COUNT_DOWN_LATCH.getCount());
    }

    public List<String> getTablesByDb(String db){
        MongoDatabase database = client.getDatabase(db);
        MongoIterable<String> strings = database.listCollectionNames();
        MongoCursor<String> iterator = strings.iterator();
        List<String> tbs = new ArrayList<>();
        strings.forEach(s->{
            tbs.add(s);
        });
        return tbs;
    }

    public void readRecs(){
        MongoDatabase database = client.getDatabase(db);
        MongoCollection<Document> collection = database.getCollection(tb);
        //long skip = this.skip;
        long read = 0;
        while (read < this.limit){
            int c;
            if (this.limit - read < readBatch){
                c = doReadRecs(this.limit - read,skip+read,collection);
                //c = doReadRecs(limit-read,collection);
            }else {
                c = doReadRecs(readBatch,skip+read,collection);
                //c = doReadRecs(readBatch,collection);
            }
            if (c == 0){
                break;
            }
            read += c;
            log.debug("==========> read {} records to block queue",c);
        }

    }

    public int doReadRecs(long limit,MongoCollection collection){
        log.debug("this limit is:{}",limit);
        if (limit == 0 || limit < 0){
            return 0;
        }
        FindIterable<Document> recs = null;
        FindIterable<Document> lastRes = null;
        ObjectId id = LAST_ID;
        synchronized (MongoDBReader.class){
            try{
                if (LAST_ID == null){
                    recs = collection.find()
                            .sort(Sorts.ascending("_id"))
                            .limit((int) limit);
                    lastRes = collection.find()
                            .sort(Sorts.ascending("_id"))
                            .skip((int) limit - 1)
                            .limit(1);
                }else {
                    recs = collection.find(Filters.gt("_id", LAST_ID))
                            .sort(Sorts.ascending("_id"))
                            .limit((int) limit);
                    lastRes = collection.find(Filters.gt("_id",LAST_ID))
                            .sort(Sorts.ascending("_id"))
                            .skip((int) limit - 1)
                            .limit(1);
                }
                MongoCursor<Document> iterator1 = lastRes.iterator();
                if (iterator1.hasNext()){
                    LAST_ID = iterator1.next().getObjectId("_id");
                }else {
                    return 0;
                }
            }catch (Exception e){
                log.error("there is an exception when read from mongodb, error msg:{},this id is :{},LAST ID is:{}",
                        e.getMessage(),id.toString(),LAST_ID.toString());
                LAST_ID = id;
                recs = null;
            }

        }
        if (recs == null){
            return 0;
        }
        MongoCursor<Document> iterator = recs.iterator();
        return dealCursor(iterator,(int) limit,0);
    }

    private int dealCursor(MongoCursor<Document> iterator,int limit,int skip){
        int count = 0;
        while (iterator.hasNext()){
            Document next = iterator.next();
            try {
                boolean result = false;
                while (!result){
                    result = recQueue.offer(next.toJson(),1, TimeUnit.SECONDS);
                }
                Total.TOTAL_READ_NUM.addAndGet(1);
                log.debug("==========> offer record to queue success:{}",next.get("_id").toString());
            } catch (Exception e) {
                log.error("there is an exception when write record to block queue,record content: {}, error message: {}",
                        next.toJson(),
                        e.getMessage());
                Total.TOTAL_FAILED_NUM.addAndGet(1);
                count--;
            }
            count++;
        }
        if (count != limit){
            log.warn("read error,count:{},limit:{},skip:{}",count,limit,skip);
        }
        iterator.close();
        return count;

    }


    public int doReadRecs(long limit,long skip,MongoCollection collection){
        MongoCursor<Document> iterator = null;
        try {
            FindIterable<Document> res = collection.find().limit((int) limit).skip((int) skip);
            iterator = res.iterator();
        }catch (Exception e){
            log.error("there is an exception when read from mongodb, error msg:{}",
                    e.getMessage());
        }
        if (iterator == null){
            return 0;
        }
        return dealCursor(iterator,(int) limit,(int) skip);
    }
}
