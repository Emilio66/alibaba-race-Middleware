package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;
import org.apache.log4j.Logger;

/**
 * 存储流程：
 * 1. 以concurrentHashMap为数据来源，因为bolt在写数据，这边在读数据，需并发控制
 * 2. 为了防止插入重复数据，可以将存入的数据从hashMap中删除，当他要更新时，再重新插入，这样减少冗余操作
 */
public class TairOperatorImpl {

    public static Logger LOG = Logger.getLogger(TairOperatorImpl.class);
    private DefaultTairManager tairManager;
    private int namespace;
    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
        LOG.debug(" new tair operator "+masterConfigServer+", "+slaveConfigServer
                +", "+groupName+", namespace"+namespace);

        this.namespace = namespace;
        this.tairManager = new DefaultTairManager();
        List<String> cs = new ArrayList<>();
        cs.add(masterConfigServer);
        cs.add(slaveConfigServer); //local, no slave

        tairManager.setConfigServerList(cs);
        tairManager.setGroupName(groupName);
        tairManager.init();
    }
    public static TairOperatorImpl newInstance(){
        return new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
    }

    public boolean write(Serializable key, Serializable value) {
        LOG.debug("write data [ "+key+" : "+value+" ]");
        tairManager.put(namespace, key, value);
        return false;
    }

    public Object get(Serializable key) {
        return tairManager.get(namespace, key);
    }

    public ResultCode remove(Serializable key) {
        return tairManager.removeItems(namespace,key,0,1);
    }

    public void close(){
        tairManager.close();
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
