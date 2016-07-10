package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.*;
import com.alibaba.middleware.race.jstorm.spout.HashSpout;
import com.alibaba.middleware.race.jstorm.spout.InputSpout;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.LocalCluster;

import java.util.HashMap;
import java.util.Map;

/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = Logger.getLogger(RaceTopology.class);

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.put("TOPOLOGY_WORKERS",2);
        conf.setNumAckers(0);   //no ack
        // conf.put("user.defined.logback.conf", "classpath:logback.xml");
        //int dispatch_Parallelism_hint = 1;

        int hash_spout_parallelism_hint = 2;
        int hash_bolt_parallelism_hint = 6;
        int dispatch_bolt_parallelism = 1;
        int count_Parallelism_hint = 1;
        int middle_bolt_parallelism =1;

        TopologyBuilder builder = new TopologyBuilder();

        // build spout
        builder.setSpout(RaceConfig.InputSpoutName, new HashSpout(), hash_spout_parallelism_hint);

        // ratio related bolt
        builder.setBolt("violentRatio", new ViolentRatioBolt(),1).shuffleGrouping(RaceConfig.InputSpoutName);
        builder.setBolt("ratioSave", new RatioSaveBolt(),1).shuffleGrouping("violentRatio");

        // order related bolt

        // emit tmallStream and taobaoStream
        builder.setBolt(RaceConfig.SPLIT_BOLT_NAME, new SplitBolt(), 1).shuffleGrouping(RaceConfig.InputSpoutName);


//        builder.setBolt("middle", new MiddleBolt(), middle_bolt_parallelism).
//                shuffleGrouping(RaceConfig.InputSpoutName);
//        //builder.setBolt(RaceConfig.HashBoltName, new HashBolt(), hash_bolt_parallelism_hint).setNumTasks(1)
//        //        .fieldsGrouping("middle", RaceConfig.HASH_STREAM, new Fields("orderId"));
//
        //tmall data process
        builder.setBolt(RaceConfig.TMDispatchBoltName, new TmallDispatchBolt(), dispatch_bolt_parallelism).setNumTasks(1).
              shuffleGrouping(RaceConfig.SPLIT_BOLT_NAME, RaceConfig.tmallStream);//hash bolt emits different streams
//        builder.setBolt(RaceConfig.TMCountBoltName, new TmallCountBolt(), count_Parallelism_hint).setNumTasks(1).
//                shuffleGrouping("middle", RaceConfig.tmallStream).
//                shuffleGrouping("middle", RaceConfig.payStream);
//
        //taobao data process
        builder.setBolt(RaceConfig.TBDispatchBoltName, new TaobaoDispatchBolt(), dispatch_bolt_parallelism).setNumTasks(1).
              shuffleGrouping(RaceConfig.SPLIT_BOLT_NAME, RaceConfig.taobaoStream);
//        builder.setBolt(RaceConfig.TBCountBoltName, new TaobaoCountBolt(), count_Parallelism_hint).setNumTasks(1).
//                shuffleGrouping("middle", RaceConfig.taobaoStream).
//                shuffleGrouping("middle", RaceConfig.payStream);
//                //fieldsGrouping(RaceConfig.TBDispatchBoltName, new Fields("minute"));
//
//        //save taobao/ tmall to tair
//        builder.setBolt("orderSave", new OrderSaveBolt(),1).setNumTasks(1).
//                shuffleGrouping(RaceConfig.TMCountBoltName, RaceConfig.TMALL_DISPATCH_STREAM).
//                shuffleGrouping(RaceConfig.TBCountBoltName, RaceConfig.TAOBAO_DISPATCH_STREAM);


        try {
            String topologyName = RaceConfig.JstormTopologyName;
               StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            // TairClient 2.3.5: init config failed.submitTopology(topologyName, conf, builder.createTopology());
            LOG.info("Succesfully start topology with config: " + conf);
        } catch (Exception e) {
            LOG.info("Submit topology error!!", e);
            e.printStackTrace();
        }
    }
}
