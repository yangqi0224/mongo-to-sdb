package com.sdb.config;

/**
 * @ClassName ConfigInfo
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 14:22
 * @Version 1.0
 **/
public class ConfigInfo {
    private String mongoUser;
    private String mongoPwd;
    private String sdbUser;
    private String sdbPwd;
    private String mongoUrl;
    private String sdbUrl;
    private String sourceDb;
    private String sourceTb;
    private String targetDb;
    private String targetTb;
    private int readThreads;
    private int writeThreads;
    private int blockThreadRadio;
    private int readBatchSize;
    private int writeBatchSize;
    private int speedDelay;
    private String collectionOption;
    private String collectionSpaceOption;

    public String getCollectionOption() {
        return collectionOption;
    }

    public void setCollectionOption(String collectionOption) {
        this.collectionOption = collectionOption;
    }

    public String getCollectionSpaceOption() {
        return collectionSpaceOption;
    }

    public void setCollectionSpaceOption(String collectionSpaceOption) {
        this.collectionSpaceOption = collectionSpaceOption;
    }

    public int getSpeedDelay() {
        return speedDelay;
    }

    public void setSpeedDelay(int speedDelay) {
        this.speedDelay = speedDelay;
    }

    public int getBlockThreadRadio() {
        return blockThreadRadio;
    }

    public void setBlockThreadRadio(int blockThreadRadio) {
        this.blockThreadRadio = blockThreadRadio;
    }

    public int getReadBatchSize() {
        return readBatchSize;
    }

    public void setReadBatchSize(int readBatchSize) {
        this.readBatchSize = readBatchSize;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public void setWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }

    public String getMongoUser() {
        return mongoUser;
    }

    public void setMongoUser(String mongoUser) {
        this.mongoUser = mongoUser;
    }

    public String getMongoPwd() {
        return mongoPwd;
    }

    public void setMongoPwd(String mongoPwd) {
        this.mongoPwd = mongoPwd;
    }

    public String getSdbUser() {
        return sdbUser;
    }

    public void setSdbUser(String sdbUser) {
        this.sdbUser = sdbUser;
    }

    public String getSdbPwd() {
        return sdbPwd;
    }

    public void setSdbPwd(String sdbPwd) {
        this.sdbPwd = sdbPwd;
    }

    public String getMongoUrl() {
        return mongoUrl;
    }

    public void setMongoUrl(String mongoUrl) {
        this.mongoUrl = mongoUrl;
    }

    public String getSdbUrl() {
        return sdbUrl;
    }

    public void setSdbUrl(String sdbUrl) {
        this.sdbUrl = sdbUrl;
    }

    public String getSourceDb() {
        return sourceDb;
    }

    public void setSourceDb(String sourceDb) {
        this.sourceDb = sourceDb;
    }

    public String getSourceTb() {
        return sourceTb;
    }

    public void setSourceTb(String sourceTb) {
        this.sourceTb = sourceTb;
    }

    public String getTargetDb() {
        return targetDb;
    }

    public void setTargetDb(String targetDb) {
        this.targetDb = targetDb;
    }

    public String getTargetTb() {
        return targetTb;
    }

    public void setTargetTb(String targetTb) {
        this.targetTb = targetTb;
    }

    public int getReadThreads() {
        return readThreads;
    }

    public void setReadThreads(int readThreads) {
        this.readThreads = readThreads;
    }

    public int getWriteThreads() {
        return writeThreads;
    }

    public void setWriteThreads(int writeThreads) {
        this.writeThreads = writeThreads;
    }
}
