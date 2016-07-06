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
    private ConcurrentHashMap<Long, Double> hashMap;
    private ConcurrentHashMap<Long, Long> hashMap1;
    private boolean isR= true;
    private static Logger Log = Logger.getLogger(PersistThread.class);
    public PersistThread(String prefix, ConcurrentHashMap<Long, Double> hashMap){
        this.prefix = prefix;
        this.hashMap = hashMap;
	Log.info("New Persist Thread for "+prefix+" , size of map: "+hashMap);
    }

    public PersistThread(boolean isRatio,String prefix, ConcurrentHashMap<Long, Long> hashMap){
        this.prefix = prefix;
        isR = false;
        this.hashMap1 = hashMap;
        Log.info("New Persist Thread for "+prefix+" , size of map: "+hashMap);
    }
    @Override
    public void run() {
        //tair operator
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        if(isR)
        for (Map.Entry<Long, Double> entry : hashMap.entrySet()) {
            Long key = entry.getKey();
            Double value = entry.getValue();

            Log.debug(key + " : " + value);         //log
            tairOperator.write(prefix +"_"+ key, value);   //persist
        }
        else
            for (Map.Entry<Long, Long> entry : hashMap1.entrySet()) {
                Long key = entry.getKey();
                Double value = entry.getValue()/100.0;

                Log.debug(key + " : " + value);         //log
                tairOperator.write(prefix +"_"+ key, value);   //persist
            }
    }
}
