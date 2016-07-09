package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.PersistThread;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.Utils.Arith;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoz on 2016/7/2.
 * 并发存入统计数据到hashMap中， 定时存入 Tair
 */
public class TmallCountBolt implements IRichBolt{

    private static final Logger Log = Logger.getLogger(TmallCountBolt.class);
    private OutputCollector collector;
    private HashMap<Long, Long> hashMap = new HashMap<Long, Long>(1024); //计数表
    private TairOperatorImpl tairOperator;
    private String prefix;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        tairOperator = TairOperatorImpl.newInstance();
        prefix = RaceConfig.prex_tmall;
    }

    @Override
    public void execute(Tuple tuple) {
        long minute = tuple.getLong(0) * 60; //1st second stands for this minute
        long payAmount = tuple.getLong(1);

       // Log.info("TmallCountBolt get [minute " + minute + " ￥" + payAmount + " ]");

        Long currentMoney = hashMap.get(minute);
        if (currentMoney == null)
            currentMoney = 0L;
        currentMoney += payAmount;  //累加金额
        //保留两位小数 （暂时去掉
        // currentMoney = Arith.round(currentMoney, 2);

        hashMap.put(minute, currentMoney);

        //save to tair directly
        tairOperator.write(prefix+"_"+minute, currentMoney / 100.0); //存入时，保留两位小数
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}


