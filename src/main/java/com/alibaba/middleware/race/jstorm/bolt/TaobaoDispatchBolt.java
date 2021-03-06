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

import java.util.HashMap;
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

    /*
    private int count = 0;
    private long currentTime = 0L;
    private long currentMoney = 0L;
    */

    private String prefix = RaceConfig.prex_taobao;
    private TairOperatorImpl tairOperator;

    //unique payment set, same hashcode but not equal
    private Set<PaymentTuple> distinctSet = new HashSet<PaymentTuple>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        tairOperator = TairOperatorImpl.newInstance();
    }

    /*
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
    */

    // 用于存储的Map
    private Map<Long, Long> amountMap = new HashMap<Long, Long>();

    // 用于标识之前正在操作的Min, 用于判别什么时候写Tair
    private long currentMin = 0L;

    @Override
    public void execute(Tuple tuple) {
        //cast to payment tuple
        PaymentTuple payment = (PaymentTuple) tuple.getValue(0);

        if(!distinctSet.contains(payment)) {
            distinctSet.add(payment);
            long minute = payment.getCreateTime();
            long amount = payment.getPayAmount();

            // 首先更新Map中的统计值
            if (amountMap.get(minute) == null) {
                amountMap.put(minute, amount);
            } else {
                amountMap.put(minute, amountMap.get(minute) + amount);
            }

            // ---- 下面开始写入Tair的逻辑 -----
            // 这里的逻辑可以处理payment乱序的情况.

            // 首先初始化currentMin
            if (currentMin == 0) {
                currentMin = minute;
            }

            if (minute > currentMin) {
                // 判断是否跳分钟了. 如果跳了, 则写入上一个数据
                for (long min = currentMin - 5; min <= currentMin; ++min) {
                    if (amountMap.containsKey(min)) {
                        flushAmountInMinute(amountMap.get(min), min);
                    }
                }
                // 当前分钟需要更新
                currentMin = minute;
            } else if (minute < currentMin) {
                // 说明写入了老的分钟数据, 那一条数据需要在tair上被更新. 不需要更新当前时间
                //flushAmountInMinute(amountMap.get(minute), minute);
            } else {
                // 说明当前分钟还在写入, 不需要进行操作.
            }

            // 写入最后一条数据
            if (amountMap.size() == 181) {
                flushAmountInMinute(amountMap.get(minute), minute);
            }

        }
    }

    /**
     * 向tair刷新某分钟的统计数值
     * @param amount
     * @param minute
     */
    private void flushAmountInMinute(long amount, long minute) {
        tairOperator.write(prefix + "_" + minute * 60, amount / 100.0); //存入时，保留两位小数
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
