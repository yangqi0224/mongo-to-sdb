package com.sdb.transfer;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.sdb.auth.mongo.MongoDBConn;
import com.sdb.config.ConfigInfo;
import com.sdb.config.ConfigLoader;
import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.exception.SDBError;
import org.bson.Document;
import org.sequoiadb.bson.BSONObject;
import org.sequoiadb.bson.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @ClassName Prepare
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 16:04
 * @Version 1.0
 **/
public class Prepare {
    private final static Logger log = LoggerFactory.getLogger(Prepare.class);

    public static boolean checkAll(){
        log.info("运行前检查");
        ConfigInfo configInfo = ConfigLoader.getConfigInfo();
        try {
            MongoClient conn = MongoDBConn.getConn();
            if (checkMongo(conn,configInfo.getSourceDb())){
                return true;
            }
        } catch (Exception e) {
            log.error("there is an exception when checking mongoDB, error message: {}",e.getMessage());
        }

        return false;
    }

    private static boolean checkMongo(MongoClient client,String db){
        try {
            MongoDatabase database = client.getDatabase(db);
            log.info("MongoDB 端数据库{}检测通过",db);
        }catch (Exception e){
            log.error("MongoDB 端检测不通过，错误信息：{}",e.getMessage());
            return false;
        }

        return true;
    }

    public static boolean checkMongo(MongoClient client,String db,String tb){
        AtomicReference<Boolean> flag = new AtomicReference<>(false);
        try {
            MongoDatabase database = client.getDatabase(db);
            MongoIterable<String> strings = database.listCollectionNames();
            strings.forEach(s->{
                if (s.equals(tb)){
                    flag.set(true);
                }
            });
            log.info("MongoDB 端数据库{},集合{},检测通过",db,tb);
        }catch (Exception e){
            log.error("MongoDB 端检测不通过，错误信息：{}",e.getMessage());
            return false;
        }
        log.info("=====>检测结果：{}",flag.get());
        return flag.get();
    }

    public static long getTotalNum(String db,String tb){
        long totalNum = -1;
        MongoClient client = null;
        try {
            client = MongoDBConn.getConn();
            MongoDatabase database = client.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(tb);
            totalNum = collection.countDocuments();
        }catch (Exception e){
            log.error("MongoDB 端检测不通过，错误信息：{}",e.getMessage());
        }finally {
            MongoDBConn.releaseConn(client);
        }
        return totalNum;
    }


    public static boolean checkSdb(Sequoiadb sdb,String db,String tb){
        try{
            CollectionSpace collectionSpace = sdb.getCollectionSpace(db);
            collectionSpace.getCollection(tb);
            log.info("SequoiaDB 端集合空间{}，集合{}检测通过。",db,tb);
        }catch (BaseException e){
            if (e.getErrorCode() == SDBError.SDB_DMS_NOTEXIST.getErrorCode()){
                log.warn("集合不存在，创建集合");
                String clOpt = ConfigLoader.getConfigInfo().getCollectionOption();
                BSONObject parse = (BSONObject) JSON.parse(clOpt);
                sdb.getCollectionSpace(db).createCollection(tb,parse);
                return checkSdb(sdb,db,tb);
            }else if (e.getErrorCode() == SDBError.SDB_DMS_CS_NOTEXIST.getErrorCode()){
                log.warn("集合空间不存在，创建集合空间");
                String csOpt = ConfigLoader.getConfigInfo().getCollectionSpaceOption();
                BSONObject parse = (BSONObject) JSON.parse(csOpt);
                sdb.createCollectionSpace(db,parse);
                return checkSdb(sdb,db,tb);
            }
            log.error("SequoiaDB 端检测不通过，错误信息：{}",e.getMessage());
            return false;
        }
        return true;

    }
}
