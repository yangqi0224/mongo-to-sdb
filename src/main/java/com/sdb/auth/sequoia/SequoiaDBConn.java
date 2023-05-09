package com.sdb.auth.sequoia;

import com.sdb.config.ConfigInfo;
import com.sdb.config.ConfigLoader;
import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.datasource.ConnectStrategy;
import com.sequoiadb.datasource.DatasourceOptions;
import com.sequoiadb.datasource.SequoiadbDatasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName SequoiaDBConn
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 13:56
 * @Version 1.0
 **/
public class SequoiaDBConn {
    private static ConfigInfo CONFIG_INFO  = ConfigLoader.getConfigInfo();
    private static List<String> SDB_ADDR = new ArrayList<>();
    private static String USER_NAME;
    private static String USER_PASSWD;
    private static SequoiadbDatasource SDB_CONN_POOL;
    private static DatasourceOptions datasourceOptions;
    private static ConfigOptions configOptions;

    private final static Logger log = LoggerFactory.getLogger(SequoiaDBConn.class);

    static {
        //配置集群连接地址
        String sdbUrl = CONFIG_INFO.getSdbUrl();
        String[] split = sdbUrl.split(",");
        for (String url:split){
            SDB_ADDR.add(url);
        }
        //用户信息
        USER_NAME = CONFIG_INFO.getSdbUser();
        USER_PASSWD = CONFIG_INFO.getSdbPwd();
        /**
         * 初始化连接池
         */
        //连接参数
        configOptions = new ConfigOptions();
        //连接超时时间
        configOptions.setConnectTimeout(500);
        //设置为长连接，true表示长连接，false为短连接
        configOptions.setSocketKeepAlive(true);
        //是否启用ssl
        configOptions.setUseSSL(false);
        //是否启用nagle算法
        configOptions.setUseNagle(false);
        //自动重试连接时间，0表示不重试
        configOptions.setMaxAutoConnectRetryTime(0);

        //连接池参数
        datasourceOptions = new DatasourceOptions();
        //最大连接数
        datasourceOptions.setMaxCount(CONFIG_INFO.getWriteThreads()*2+5);
        //最大空闲连接数
        datasourceOptions.setMaxIdleCount(CONFIG_INFO.getWriteThreads());
        //每隔一天将连接池中多于MaxIdleCount限定的空闲连接关闭，
        datasourceOptions.setCheckInterval(24*3600*1000);
        //每次新增连接时的数量
        datasourceOptions.setDeltaIncCount(1);
        // 池中空闲连接存活时间。单位:毫秒。0表示不关心连接隔多长时间没有收发消息。
        datasourceOptions.setKeepAliveTimeout(0);
        //出池时是否检测连接是否有效
        datasourceOptions.setValidateConnection(true);
        //向编目节点同步coord连接地址的时间间隔，0表示不同不同步
        datasourceOptions.setSyncCoordInterval(0);
        //默认使用coord地址负载均衡的策略获取连接
        datasourceOptions.setConnectStrategy(ConnectStrategy.BALANCE);
        //构建连接池
        init();
    }


    public static void init(){
        SDB_CONN_POOL = new SequoiadbDatasource(SDB_ADDR,USER_NAME,USER_PASSWD,configOptions,datasourceOptions);
    }


    /**
     * 获取SequoiaDB数据库连接
     * @return
     */
    public static Sequoiadb getConnection() throws InterruptedException {
        return SDB_CONN_POOL.getConnection();
    }

    /**
     * 回收连接
     * @param sdb
     */
    public static void releaseConnection(Sequoiadb sdb){
        SDB_CONN_POOL.releaseConnection(sdb);
    }

    /**
     * 关闭连接池
     */
    public static void closeResource(){
        SDB_CONN_POOL.close();
    }

    public static Sequoiadb resetConn(Sequoiadb sequoiadb) throws InterruptedException {
        try {
            return getConnection();
        }catch (InterruptedException e){
            throw e;
        }finally {
            sequoiadb.close();
            try {
                //releaseConnection(sequoiadb);
            }catch (Exception e){
                log.error("release connection failed , error message:{}",e.getMessage());
            }
        }
    }


}
