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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoz on 2016/7/3.
 */
public class TaobaoCountBolt implements IRichBolt {
    private OutputCollector collector;
    private static final Logger Log = Logger.getLogger(TaobaoCountBolt.class);
    //计数表,Long计算，存时除100.0;需要并发，bolt写，thread 读，然后存tair;
    private static HashMap<Long, Long> hashMap = new HashMap<Long, Long>();
    private static ScheduledThreadPoolExecutor scheduledPersist = new ScheduledThreadPoolExecutor(RaceConfig.persistThreadNum);
    private static HashSet<Integer> distinctSet = new HashSet<Integer>(1024);
    private TairOperatorImpl tairOperator;
    private String prefix;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        /*scheduledPersist.scheduleAtFixedRate( new PersistThread(RaceConfig.prex_taobao, hashMap),
                RaceConfig.persistInitialDelay, RaceConfig.persitInterval, TimeUnit.SECONDS);*/
        this.tairOperator = TairOperatorImpl.newInstance();
        prefix = RaceConfig.prex_taobao;
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

            //Double price = payAmount / 100.0; //change to double
            Long currentMoney = hashMap.get(createTime);

            if (currentMoney == null)
                currentMoney = 0L;
            currentMoney += payAmount;  //累加金额
            //保留两位小数 （暂时去掉
            // currentMoney = Arith.round(currentMoney, 2);

           // Log.debug("TaobaoCountBolt get [min: " + createTime + ", ￥" + payAmount + ", current sum ￥ " + currentMoney + "]");
            hashMap.put(createTime, currentMoney);
            distinctSet.add(paymentTuple.hashCode());
            //save to tair directly
            tairOperator.write(prefix + "_" +createTime, currentMoney / 100.0); //存入时，保留两位小数
        } else {
            Log.debug("Already processed: " + paymentTuple.hashCode() + " : " + paymentTuple);
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
