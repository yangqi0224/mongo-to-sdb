package com.sdb;

import com.sdb.auth.mongo.MongoDBConn;
import com.sdb.auth.sequoia.SequoiaDBConn;
import com.sdb.common.Utils;
import com.sdb.config.ConfigInfo;
import com.sdb.config.ConfigLoader;
import com.sdb.config.ConfigProperties;
import com.sdb.exe.Executor;
import com.sdb.stat.Total;
import com.sdb.transfer.MongoDBReader;
import com.sdb.transfer.Prepare;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @ClassName Executor
 * @Description 执行器
 * @Author yangqi
 * @Date 2023/3/15 17:14
 * @Version 1.0
 **/
public class Entry {

    private final static Logger log = LoggerFactory.getLogger(Entry.class);
    public static void main(String[] args) {
        ConfigLoader.loadConfig();
        boolean b = Prepare.checkAll();
        if (!b){
            log.error("check failed,please check your database status and try again");
            throw new RuntimeException("prepare check failed.");
        }
        log.info("===========开始迁移==============");
        ConfigInfo configInfo = ConfigLoader.getConfigInfo();
        List<String> sourceTbs ;
        List<String> targetTbs ;
        //处理源、目标表名
        if (configInfo.getSourceTb().equals("") || configInfo.getSourceTb()==null){
            sourceTbs = new MongoDBReader().getTablesByDb(configInfo.getSourceDb());
            targetTbs = sourceTbs;
        }else {
            sourceTbs = Utils.stringToList(configInfo.getSourceTb(),",");
            if (configInfo.getTargetTb().equals("") || configInfo.getTargetTb()==null){
                targetTbs = sourceTbs;
            }else {
                targetTbs = Utils.stringToList(configInfo.getTargetTb(),",");
            }
        }
        //构建执行器
        Executor executor = new Executor(configInfo,sourceTbs,targetTbs);
        try {
            executor.init().start();
            log.info("===========迁移结束==============");
        } catch (InterruptedException e) {
            log.error("there is a exception when transfer record, error message : {}",e.getMessage());
        }finally {
            //打印迁移统计信息
            Total.ALL_STATS.forEach(k->{
                log.info(k);
            });
            try {//关闭资源
                SequoiaDBConn.closeResource();
                MongoDBConn.closeResource();
            }catch (Exception e){
                log.error("there is an exception when closing session pool,error message :{}",e.getMessage());
            }
        }
        System.exit(0);
    }

}
