package com.sdb.auth.mongo;


import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.sdb.config.ConfigInfo;
import com.sdb.config.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @ClassName MongoDBConn
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 13:56
 * @Version 1.0
 **/
public class MongoDBConn {
    private final static Logger log = LoggerFactory.getLogger(MongoDBConn.class);

    private static ConfigInfo configInfo;
    private static ArrayBlockingQueue<MongoClient> connPool;


    static {
        configInfo = ConfigLoader.getConfigInfo();
        connPool = new ArrayBlockingQueue(configInfo.getReadThreads()*2);
    }

    public static MongoClient getConn(){
        MongoClient mongoClient = connPool.poll();
        try {
            if (mongoClient == null){
                mongoClient = MongoClients.create(configInfo.getMongoUrl());
            }
        } catch (Exception e) {
            log.error("there is an exception when creating mongoDB connection,error message:{}",e.getMessage());
        }
        return mongoClient;
    }

    public static void releaseConn(MongoClient client){
        if (connPool.size() == configInfo.getReadThreads()*2){
            client.close();
        }else {
            connPool.offer(client);
        }
    }

    public static void closeResource(){
        connPool.forEach(mongoClient -> {
            mongoClient.close();
        });
    }
}
