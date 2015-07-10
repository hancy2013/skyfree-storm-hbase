package com.skyfree.storm.hbase;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/10 18:46
 */
public class HbaseSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;

    private static final Map<Integer, String> FIRST_NAME_MAP = new HashMap<Integer, String>();

    static {
        FIRST_NAME_MAP.put(0, "john");
        FIRST_NAME_MAP.put(1, "nick");
        FIRST_NAME_MAP.put(2, "mick");
        FIRST_NAME_MAP.put(3, "tom");
        FIRST_NAME_MAP.put(4, "jerry");
    }

    private static final Map<Integer, String> LAST_NAME_MAP = new HashMap<Integer, String>();

    static {
        LAST_NAME_MAP.put(0, "anderson");
        LAST_NAME_MAP.put(1, "watson");
        LAST_NAME_MAP.put(2, "ponting");
        LAST_NAME_MAP.put(3, "dravid");
        LAST_NAME_MAP.put(4, "lara");
    }

    private static final Map<Integer, String> COMPANY_NAME_MAP = new HashMap<Integer, String>();

    static {
        COMPANY_NAME_MAP.put(0, "abc");
        COMPANY_NAME_MAP.put(1, "dfg");
        COMPANY_NAME_MAP.put(2, "pqr");
        COMPANY_NAME_MAP.put(3, "ecd");
        COMPANY_NAME_MAP.put(4, "awe");
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("firstName", "lastName", "companyName"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        final Random random = new Random();

        int randomNumber = random.nextInt(5);

        this.collector.emit(new Values(
                        FIRST_NAME_MAP.get(randomNumber),
                        LAST_NAME_MAP.get(randomNumber),
                        COMPANY_NAME_MAP.get(randomNumber)
                )
        );

    }
}
