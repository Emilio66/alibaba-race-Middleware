package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //自己添加的
    public static String teamCode           = "424452my9i";  //新要求，加入teamcode进行区分，注意结尾有下划线

    public static String groupName          = "singularity";
    public static String nameServer         = "127.0.0.1:9876";
    public static boolean isFlowControl     = false; //调优用, 流量控制，消息缓存
    public static boolean autoACK           = true; //调优用，自动ACK
    public static int     maxFailTime       = 4;    //调优用，重试次数
    public static int     persistThreadNum  = 1;
    public static int     persitInterval    = 3;    //seconds
    public static int     persistInitialDelay   = 10;   //seconds, 启动后延迟一段时间再开始执行
    //public static String

    //这些是写tair key的前缀
    public static String  prex_tmall            = "platformTmall_"+teamCode;
    public static String  prex_taobao           = "platformTaobao_"+teamCode;
    public static String  prex_ratio            = "ratio_"+teamCode;

    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布

    public static String  JstormTopologyName    = "424452my9i";
    public static String  MetaConsumerGroup     = "424452my9i";    //RabbitMQ consumer group
    public static String  MqPayTopic            = "MiddlewareRaceTestData_Pay"; //不同的topic
    public static String  MqTmallTradeTopic     = "MiddlewareRaceTestData_TBOrder";
    public static String  MqTaobaoTradeTopic    = "MiddlewareRaceTestData_TMOrder";
    public static String  TairConfigServer      = "10.101.72.127:5198";
    public static String  TairSalveConfigServer = "10.101.72.128:5198";
    public static String  TairGroup             = "group_tianchi";
    public static Integer TairNamespace         = 25469;

    public static String tmallStream = "tmall";
    public static String taobaoStream = "taobao";
    public static String payStream = "pay";
}
