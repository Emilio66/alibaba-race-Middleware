package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.PersistThread;
import com.alibaba.middleware.race.Utils.Arith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static final Logger LOG = LoggerFactory.getLogger(PayRatioBolt.class);

    private static ConcurrentHashMap<Long, Long> mobileMap = new ConcurrentHashMap<Long, Long>();
    private static ConcurrentHashMap<Long, Long> pcMap = new ConcurrentHashMap<Long, Long>();
    private static ConcurrentHashMap<Long, Double> ratioMap = new ConcurrentHashMap<Long, Double>();

    private static ScheduledThreadPoolExecutor scheduledPersist = new ScheduledThreadPoolExecutor(RaceConfig.persistThreadNum);

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
        Long amount = tuple.getLong(1);
        Short platform = tuple.getShort(2);

        LOG.debug("PayRatioBolt get [min: "+minute+", ￥"+amount+", platform: "+platform+"]");
        //pc
        if(platform == 0){
            Long pcAmount = pcMap.get(minute);
            if (pcAmount == null){

                //上一分钟的历史交易额作为起点
                if(( pcAmount = pcMap.get(minute - 60)) == null) {
                    pcAmount = 0L;
                }
            }

            pcAmount += amount; //加上历史交易作为总交易额
            pcMap.put(minute, pcAmount);

            //计算比值
            Long mobileAmount = mobileMap.get(minute);
            if (mobileAmount == null)
                mobileAmount = 0L;
            double ratio = Arith.div(mobileAmount*1.0, pcAmount*1.0, 2);//精确除法,保留2位
            ratioMap.put(minute,ratio);

        }else{
            //无线端交易
            Long mobileAmount = mobileMap.get(minute);
            if (mobileAmount == null){

                //上一分钟的历史交易额作为起点
                if(( mobileAmount = mobileMap.get(minute - 60)) == null) {
                    mobileAmount = 0L;
                }
            }

            mobileAmount += amount; //加上历史交易作为总交易额
            mobileMap.put(minute, mobileAmount);

            //计算比值
            Long pcAmount = pcMap.get(minute);
            Double ratio = Double.MAX_VALUE; //pc 端为 0， 比值无限大
            if (pcAmount != null) {
                ratio = Arith.div(mobileAmount*1.0, pcAmount*1.0, 2);//精确除法,保留2位
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
