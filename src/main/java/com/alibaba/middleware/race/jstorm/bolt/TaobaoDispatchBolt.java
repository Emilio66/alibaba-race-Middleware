package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaoz on 2016/7/3.
 * 1. 全局缓存处理过的 payment object, payment消息判重(hashcode效率高，但可能误判,在equals方法进行二次判断)
 * 2. 拿到这个payment, 按分钟区分，发送给bolt去统计
 */
public class TaobaoDispatchBolt implements IRichBolt{
    private OutputCollector collector;
    private static final Logger LOG = Logger.getLogger(TaobaoDispatchBolt.class);
    private int count = 0;
    private long currentTime = 0L;
    private long currentMoney = 0L;
    private String prefix = RaceConfig.prex_taobao;
    private TairOperatorImpl tairOperator;

    //unique payment set, same hashcode but not equal
    //private Set<PaymentTuple> distinctSet = new HashSet<PaymentTuple>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        tairOperator = TairOperatorImpl.newInstance();
    }

    @Override
    public void execute(Tuple tuple) {
        //cast to payment tuple
        PaymentTuple payment = (PaymentTuple) tuple.getValue(0);

        long minute= payment.getCreateTime();
        long money = payment.getPayAmount();

        if (currentTime == 0) {
            currentTime = minute;
        }

        if(minute > currentTime){
            long savedTime = minute * 60;
            double savedMoney = currentMoney / 100.0;
            //write last minute
            tairOperator.write(prefix+"_"+savedTime, savedMoney); //存入时，保留两位小数
            currentTime = minute;
            currentMoney = 0;   //清零
            ++count;
        }
        currentMoney += money;
        if(count == 181)
            tairOperator.write(prefix+"_"+minute*60,currentMoney/100.0); //存入时，保留两位小数
      //  LOG.info("TaobaoDispatchBolt get "+payment);
        //去重
        //if(!distinctSet.contains(payment)){

          //  distinctSet.add(payment);
            //only emit useful fields [minute, payAmount, payPlatform] to count bolts
            //collector.emit(new Values(payment.getCreateTime(),  payment.getPayAmount(), payment.getPayPlatform()));

          //  LOG.info("TaobaoDispatchBolt emit "+payment);
        //}
        //collector.ack(tuple);
    }

    //declare useful fields
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("minute", "price", "platform"));
    }

    @Override
    public void cleanup() {

    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
