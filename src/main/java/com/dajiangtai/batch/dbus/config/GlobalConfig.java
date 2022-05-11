package com.dajiangtai.batch.dbus.config;

import java.io.Serializable;

/**
 * 在生产上一般通过配置中心来管理
 */
public class GlobalConfig implements Serializable {
    /**
     * 数据库driver class
     */
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    /**
     * 数据库jdbc url
     */
    public static final String DB_URL = "jdbc:mysql://192.168.20.211:3306/test?useUnicode=true&characterEncoding=utf8";
    /**
     * 数据库user name
     */
    public static final String USER_MAME = "canal";
    /**
     * 数据库password
     */
    public static final String PASSWORD = "canal";
    /**
     * 批量提交size
     */
    public static final int BATCH_SIZE = 2;

    //HBase相关配置
    public static final String HBASE_ZOOKEEPER_QUORUM = "master,slave1,slave2";
    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181";
    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase";

}
