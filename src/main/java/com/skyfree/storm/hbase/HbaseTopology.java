package com.skyfree.storm.hbase;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/10 19:00
 */
public class HbaseTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        List<String> zks = new ArrayList<String>();
        zks.add("127.0.0.1");

        List<String> cFs = new ArrayList<String>();
        cFs.add("personal");
        cFs.add("company");

        builder.setSpout("hbase_spout", new HbaseSpout(), 2);
        builder.setBolt("hbase_bolt", new HbaseBolt("user", cFs, zks, 2181), 2).shuffleGrouping("hbase_spout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("hbase_topology", config, builder.createTopology());

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            System.out.println("Thread interrupted:" + e);
        }

        System.out.println("stopped called...");

        cluster.killTopology("hbase_topology");

        cluster.shutdown();
    }
}
