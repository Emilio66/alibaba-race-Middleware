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
 * 计算 无线/PC 历史总交易额在这一分钟时的比值
 * 1. 分别统计这一分钟 无线与PC 的交易额
 * 2. 将这一分钟的交易额加上历史的交易额存入 hashMap 中
 * 3. 将两个 hashMap 这一分钟的值相除，取两位小数存入 hashMap （可以在存入tair的时候统一计算ratio
 */
public class PayRatioBolt implements IRichBolt{
    private OutputCollector collector;
    public static final Logger Log = Logger.getLogger(PayRatioBolt.class);

    public static ConcurrentHashMap<Long, Double> mobileMap = new ConcurrentHashMap<Long, Double>();
    public static ConcurrentHashMap<Long, Double> pcMap = new ConcurrentHashMap<Long, Double>();
    public static ConcurrentHashMap<Long, Double> ratioMap = new ConcurrentHashMap<Long, Double>();
    public static ScheduledThreadPoolExecutor scheduledPersist = new ScheduledThreadPoolExecutor(RaceConfig.persistThreadNum);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        //定时存入Tair
        scheduledPersist.scheduleAtFixedRate( new PersistThread(RaceConfig.prex_ratio, ratioMap),
                RaceConfig.persistInitialDelay, RaceConfig.persitInterval, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Tuple tuple) {
        Long minute = tuple.getLong(0);
        Double amount = tuple.getDouble(1);
        Short platform = tuple.getShort(2);

        //pc
        if(platform == 0){
          Double pcAmount = pcMap.get(minute);
            if (pcAmount == null){

                //上一分钟的历史交易额作为起点
                if(( pcAmount = pcMap.get(minute - 60)) == null) {
                    pcAmount = 0.0;
                }
            }

            pcAmount += amount; //加上历史交易作为总交易额
            pcMap.put(minute, pcAmount);

            //计算比值
            Double mobileAmount = mobileMap.get(minute);
            if (mobileAmount == null)
                mobileAmount = 0.0;
            ratioMap.put(minute, mobileAmount / pcAmount);

        }else{
            //无线端交易
            Double mobileAmount = mobileMap.get(minute);
            if (mobileAmount == null){

                //上一分钟的历史交易额作为起点
                if(( mobileAmount = mobileMap.get(minute - 60)) == null) {
                    mobileAmount = 0.0;
                }
            }

            mobileAmount += amount; //加上历史交易作为总交易额
            mobileMap.put(minute, mobileAmount);

            //计算比值
            Double pcAmount = pcMap.get(minute);
            Double ratio = Double.MAX_VALUE; //pc 端为 0， 比值无限大
            if (pcAmount != null) {
                ratio = mobileAmount / pcAmount;
            }

            ratioMap.put(minute, ratio);
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
