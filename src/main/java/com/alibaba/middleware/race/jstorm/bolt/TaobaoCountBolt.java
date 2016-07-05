package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.PersistThread;
import com.alibaba.middleware.race.Utils.Arith;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoz on 2016/7/3.
 */
public class TaobaoCountBolt implements IRichBolt{
    private OutputCollector collector;
    private static final Logger Log = Logger.getLogger(TmallCountBolt.class);
    private static HashMap<Long, Double> hashMap = new HashMap<Long, Double>(); //计数表
    private static ScheduledThreadPoolExecutor scheduledPersist = new ScheduledThreadPoolExecutor(RaceConfig.persistThreadNum);
    private static HashSet<Integer> distinctSet = new HashSet<Integer>(1024);
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
        scheduledPersist.scheduleAtFixedRate( new PersistThread(RaceConfig.prex_taobao, hashMap),
                RaceConfig.persistInitialDelay, RaceConfig.persitInterval, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Tuple tuple) {
        long orderId = tuple.getLong(0);
        long payAmount = tuple.getLong(1);
        short paySource = tuple.getShort(2);
        short platform = tuple.getShort(3);
        long createTime = tuple.getLong(4);

        Log.debug("TaoBaoBolt get [order ID: " + orderId + ", time: " + createTime
                + " ￥" + payAmount + " ]");

        //判重
        PaymentTuple paymentTuple = new PaymentTuple(orderId, payAmount, paySource, platform, createTime);

        if (!distinctSet.contains(paymentTuple.hashCode())) {

            Double price = payAmount / 100.0; //change to double
            Double currentMoney = hashMap.get(createTime);

            if (currentMoney == null)
                currentMoney = 0.0;
            currentMoney += price;  //累加金额
            //保留两位小数 （暂时去掉
            // currentMoney = Arith.round(currentMoney, 2);

            Log.debug("TaobaoCountBolt get [min: " + createTime + ", ￥" + price + ", current sum ￥ " + currentMoney + "]");
            hashMap.put(createTime, currentMoney);
            distinctSet.add(paymentTuple.hashCode());
        }
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
