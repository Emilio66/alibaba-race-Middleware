package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
//import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.TaobaoCountBolt;
import com.alibaba.middleware.race.jstorm.bolt.TaobaoDispatchBolt;
import com.alibaba.middleware.race.jstorm.bolt.TmallCountBolt;
import com.alibaba.middleware.race.jstorm.bolt.TmallDispatchBolt;
import com.alibaba.middleware.race.jstorm.spout.MainSpout;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.applet.Main;

import java.io.InputStream;
import java.util.Properties;


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

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {

        InputStream in = RaceTopology.class.getClassLoader().getResourceAsStream("application.properties");
        Properties prop = new Properties();
        prop.load(in);
        LOG.debug(prop.getProperty("log4j.rootLogger"));
        PropertyConfigurator.configure(prop);

        Config conf = new Config();
        int spout_Parallelism_hint = 1;
        int dispatch_Parallelism_hint = 1;
        int count_Parallelism_hint = 1;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new MainSpout(), spout_Parallelism_hint);

        //tmall data process
        builder.setBolt("tmallDispatch", new TmallDispatchBolt(), dispatch_Parallelism_hint).
                localOrShuffleGrouping("source", MainSpout.tmallStream);    //different stream
        builder.setBolt("tmallCount", new TmallCountBolt(), count_Parallelism_hint).
                fieldsGrouping("tmallDispatch", new Fields("minute"));

        //taobao data process
        builder.setBolt("taobaoDispatch", new TaobaoDispatchBolt(), dispatch_Parallelism_hint).
                localOrShuffleGrouping("source", MainSpout.taobaoStream);
        builder.setBolt("taobaoCount", new TaobaoCountBolt(), count_Parallelism_hint).
                fieldsGrouping("taobaoDispatch", new Fields("minute"));

        //pay ratio process

        try {
            String topologyName = RaceConfig.JstormTopologyName;
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            LOG.info("Submit topology error!!", e);
            e.printStackTrace();
        }
    }
}