package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.PersistThread;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoz on 2016/7/3.
 */
public class TaobaoCountBolt implements IRichBolt{
    private OutputCollector collector;
    public static final Logger Log = Logger.getLogger(TmallCountBolt.class);
    public static ConcurrentHashMap<Long, Double> hashMap = new ConcurrentHashMap<Long, Double>(); //计数表
    public static ScheduledThreadPoolExecutor scheduledPersist = new ScheduledThreadPoolExecutor(RaceConfig.persistThreadNum);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
        scheduledPersist.scheduleAtFixedRate( new PersistThread(RaceConfig.prex_taobao, hashMap),
                RaceConfig.persistInitialDelay, RaceConfig.persitInterval, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Tuple tuple) {
        Long minute = tuple.getLong(0);
        Double price = tuple.getDouble(1);
        Double currentMoney = hashMap.get(minute);

        if (currentMoney == null)
            currentMoney = 0.0;
        currentMoney += price;  //累加金额
        hashMap.put(minute, currentMoney);

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