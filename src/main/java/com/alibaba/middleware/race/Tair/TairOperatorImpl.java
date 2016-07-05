package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

    public static Logger LOG = LoggerFactory.getLogger(TairOperatorImpl.class);
    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
        LOG.debug(" new tair operator "+masterConfigServer+", "+slaveConfigServer
                +", "+groupName+", namespace"+namespace);
    }

    public boolean write(Serializable key, Serializable value) {
        LOG.debug("write data [ "+key+" : "+value+" ]");
        return false;
    }

    public Object get(Serializable key) {
        return null;
    }

    public boolean remove(Serializable key) {
        return false;
    }

    public void close(){
    }

    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
    }
}
