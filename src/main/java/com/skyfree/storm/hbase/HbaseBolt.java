package com.skyfree.storm.hbase;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/10 18:52
 */
public class HbaseBolt implements IBasicBolt {

    private static final long serialVersionUID = 1L;
    private HBaseOperations operations;
    private String tableName;
    private List<String> columnFamilies;
    private List<String> zookeeperIPs;
    private int zkPort;

    public HbaseBolt(String tableName, List<String> columnFamilies, List<String> zookeeperIPs, int zkPort) {
        this.tableName = tableName;
        this.columnFamilies = columnFamilies;
        this.zookeeperIPs = zookeeperIPs;
        this.zkPort = zkPort;
    }

    public void prepare(Map map, TopologyContext topologyContext) {
        this.operations = new HBaseOperations(this.tableName, this.columnFamilies, this.zookeeperIPs, this.zkPort);

    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Map<String, Map<String, Object>> record = new HashMap<String, Map<String, Object>>();
        Map<String, Object> personalMap = new HashMap<String, Object>();

        personalMap.put("firstName", tuple.getValueByField("firstName"));
        personalMap.put("lastName", tuple.getValueByField("lastName"));

        Map<String, Object> companyMap = new HashMap<String, Object>();
        companyMap.put("companyName", tuple.getValueByField("companyName"));

        record.put("personal", personalMap);
        record.put("company", companyMap);

        operations.insert(record, UUID.randomUUID().toString());
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
