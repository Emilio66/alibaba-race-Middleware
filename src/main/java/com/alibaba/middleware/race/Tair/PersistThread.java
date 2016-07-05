package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaoz on 2016/7/3.
 */
//线程： 把数据存入tair
public class PersistThread implements Runnable {

    private String prefix;
<<<<<<< HEAD
    private HashMap<Long, Double> hashMap;
    private static Logger Log = Logger.getLogger(PersistThread.class);
    public PersistThread(String prefix, HashMap<Long, Double> hashMap){
=======
    private ConcurrentHashMap<Long, Double> hashMap;
    private static Logger LOG = Logger.getLogger(PersistThread.class);
    public PersistThread(String prefix, ConcurrentHashMap<Long, Double> hashMap){
>>>>>>> 034b6885004e05a76de7feacb64c016e57247da1
        this.prefix = prefix;
        this.hashMap = hashMap;
    }

    @Override
    public void run() {
        //tair operator
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);

        for (Map.Entry<Long, Double> entry : hashMap.entrySet()) {
            Long key = entry.getKey();
            Double value = entry.getValue();

            //LOG.debug(key + " : " + value);         //log
            tairOperator.write(prefix +"_"+ key, value);   //persist
        }
    }
}