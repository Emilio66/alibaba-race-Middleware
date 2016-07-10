package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.tuple.OrderTuple;
import com.alibaba.middleware.race.jstorm.tuple.PaymentTuple;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by zhaoz on 2016/7/9.
 * spout 拿到數據就往這扔，不要寫任何的邏輯，暴力解決
 */
public class MiddleBolt  implements IRichBolt {
    private OutputCollector collector;
    private static Logger LOG = Logger.getLogger(MiddleBolt.class);
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Object field1 = input.getValue(0);
        Object field2 = input.getValue(1);

        //payment list tuple
        if(field1 != null){
            //throw out directly
            collector.emit(RaceConfig.payStream, new Values(field1));

        }else{
            //order list tuple
            ArrayList<OrderTuple>list = (ArrayList<OrderTuple>)field2;
            ArrayList<Long> idList = new ArrayList<>(list.size());

            for (OrderTuple order : list){
                idList.add(order.getOrderId());
            }
            //taobao list
            if(list.size() > 0) {
                if (list.get(0).getOrderType() == 0) {
                    collector.emit(RaceConfig.taobaoStream, new Values(idList));
                } else {
                    collector.emit(RaceConfig.tmallStream, new Values(idList));
                }
            }
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.payStream, new Fields("payList"));
        declarer.declareStream(RaceConfig.taobaoStream, new Fields("TBIdList"));
        declarer.declareStream(RaceConfig.tmallStream, new Fields("TMIdList"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
