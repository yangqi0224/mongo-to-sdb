package com.sdb.config;


import sun.awt.IconInfo;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * @ClassName ConfigLoader
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 14:08
 * @Version 1.0
 **/
public class ConfigLoader {

    private static Properties PROPERTIES = new Properties();
    private static String PROPERTIES_KEY = "sdb.transfer.config";
    private static ConfigInfo CONFIG_INFO = new ConfigInfo();

    public static void loadConfig(){
        InputStream fi = null;
        String configPath= System.getProperty(PROPERTIES_KEY,"/Users/yangqi/IdeaProjects/mongo-to-sdb/src/main/resources/config.properties");
        try {
            fi = new FileInputStream(configPath);
            PROPERTIES.load(fi);
            CONFIG_INFO.setMongoUser(PROPERTIES.getProperty(ConfigProperties.MONGO_USER));
            CONFIG_INFO.setMongoPwd(PROPERTIES.getProperty(ConfigProperties.MONGO_PWD));
            CONFIG_INFO.setMongoUrl(PROPERTIES.getProperty(ConfigProperties.MONGO_CONN_STR));
            CONFIG_INFO.setSdbUser(PROPERTIES.getProperty(ConfigProperties.SDB_USER));
            CONFIG_INFO.setSdbPwd(PROPERTIES.getProperty(ConfigProperties.SDB_PWD));
            CONFIG_INFO.setSdbUrl(PROPERTIES.getProperty(ConfigProperties.SDB_CONN_STR));
            CONFIG_INFO.setSourceDb(PROPERTIES.getProperty(ConfigProperties.SOURCE_DB));
            CONFIG_INFO.setSourceTb(PROPERTIES.getProperty(ConfigProperties.SOURCE_TB));
            CONFIG_INFO.setTargetDb(PROPERTIES.getProperty(ConfigProperties.TARGET_DB));
            CONFIG_INFO.setTargetTb(PROPERTIES.getProperty(ConfigProperties.TARGET_TB));
            CONFIG_INFO.setCollectionOption(PROPERTIES.getProperty(ConfigProperties.COLLECTION_OPTION));
            CONFIG_INFO.setCollectionSpaceOption(PROPERTIES.getProperty(ConfigProperties.COLLECTION_SPACE_OPTION));
            CONFIG_INFO.setReadThreads(Integer.valueOf(PROPERTIES.getProperty(ConfigProperties.READ_THREADS)));
            CONFIG_INFO.setWriteThreads(Integer.valueOf(PROPERTIES.getProperty(ConfigProperties.WRITE_THREADS)));
            CONFIG_INFO.setBlockThreadRadio(Integer.valueOf(PROPERTIES.getProperty(ConfigProperties.BLOCK_RADIO)));
            CONFIG_INFO.setReadBatchSize(Integer.valueOf(PROPERTIES.getProperty(ConfigProperties.READ_BATCH_SIZE)));
            CONFIG_INFO.setWriteBatchSize(Integer.valueOf(PROPERTIES.getProperty(ConfigProperties.WRITE_BATCH_SIZE)));
            CONFIG_INFO.setSpeedDelay(Integer.valueOf(PROPERTIES.getProperty(ConfigProperties.SPEED_DELAY)));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static ConfigInfo getConfigInfo() {
        return CONFIG_INFO;
    }
}
