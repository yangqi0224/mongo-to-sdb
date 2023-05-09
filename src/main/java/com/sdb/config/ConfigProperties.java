package com.sdb.config;

/**
 * @ClassName ConfigProperties
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 13:57
 * @Version 1.0
 **/
public class ConfigProperties {
    public final static String MONGO_USER = "mongo.user";
    public final static String MONGO_PWD = "mongo.pwd";
    public final static String SDB_USER = "sdb.user";
    public final static String SDB_PWD = "sdb.pwd";
    public final static String MONGO_CONN_STR = "mongo.url";
    public final static String SDB_CONN_STR = "sdb.url";
    public final static String SOURCE_DB = "source.database.name";
    public final static String SOURCE_TB = "source.table.name";
    public final static String TARGET_DB = "target.database.name";
    public final static String TARGET_TB = "target.table.name";
    public final static String READ_THREADS = "read.thread.num";
    public final static String WRITE_THREADS = "write.thread.num";
    public final static String BLOCK_RADIO = "block.thread.radio";
    public final static String READ_BATCH_SIZE = "read.batch.size";
    public final static String WRITE_BATCH_SIZE = "write.batch.size";
    public final static String SPEED_DELAY = "speed.stat.delay";
    public final static String COLLECTION_OPTION = "sequoaidb.collection.option";
    public final static String COLLECTION_SPACE_OPTION = "sequoiadb.collection.space.option";
}
