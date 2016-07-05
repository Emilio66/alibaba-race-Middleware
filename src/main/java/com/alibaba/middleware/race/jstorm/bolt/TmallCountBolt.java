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
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoz on 2016/7/2.
 * 并发存入统计数据到hashMap中， 定时存入 Tair
 */
public class TmallCountBolt implements IRichBolt {
    private OutputCollector collector;
    private static final Logger LOG = Logger.getLogger(TmallCountBolt.class);
    private static ConcurrentHashMap<Long, Double> hashMap = new ConcurrentHashMap<Long, Double>(); //计数表
    private static ScheduledThreadPoolExecutor scheduledPersist = new ScheduledThreadPoolExecutor(RaceConfig.persistThreadNum);//定时存入Tair

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
        scheduledPersist.scheduleAtFixedRate( new PersistThread(RaceConfig.prex_tmall, hashMap),
                RaceConfig.persistInitialDelay, RaceConfig.persitInterval, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Tuple tuple) {
        Long minute = tuple.getLong(0);
        Double price = tuple.getLong(1) / 100.0;
        Double currentMoney = hashMap.get(minute);

        if (currentMoney == null)
            currentMoney = 0.0;
        currentMoney += price;  //累加金额
        //保留两位小数 (暂时不用
        //currentMoney = Arith.round(currentMoney, 2);
        hashMap.put(minute, currentMoney);

        LOG.debug("TaobaoCountBolt get [min: "+minute+", ￥"+price+", current sum ￥ "+currentMoney+"]");
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


