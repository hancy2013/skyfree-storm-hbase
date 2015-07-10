package com.skyfree.storm.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/10 16:38
 */
public class HBaseOperations implements Serializable {
    private static final long serialVersionUID = 1L;

    Configuration conf = new Configuration();

    HTable hTable = null;

    @SuppressWarnings("StringBufferMayBeStringBuilder")
    public HBaseOperations(String tableName, List<String> ColumnFamilies, List<String> zookeeperIPs, int zkPort) {
        conf = HBaseConfiguration.create();

        StringBuffer zookeeperIP = new StringBuffer();

        for (String zookeeper : zookeeperIPs) {
            zookeeperIP.append(zookeeper).append(',');
        }
        zookeeperIP.deleteCharAt(zookeeperIP.length() - 1);
        System.out.println(zookeeperIP.toString());
        conf.set("hbase.zookeeper.quorum", zookeeperIP.toString());
        conf.setInt("hbase.zookeeper.property.clientPort", zkPort);
        conf.set("zookeeper.znode.parent","/hbase-unsecure");

        createTable(tableName, ColumnFamilies);

        try {
            hTable = new HTable(conf, tableName);
        } catch (IOException e) {
            System.out.println("Error occurred while creating instance of HTable class:" + e);
        }
    }

    public void createTable(String tableName, List<String> ColumnFamilies) {
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
            HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes(tableName));

            for (String columnFamily : ColumnFamilies) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                tableDescriptor.addFamily(columnDescriptor);
            }
            admin.createTable(tableDescriptor);
            admin.close();
        } catch (TableExistsException e) {
            System.out.println("Table already exist:" + tableName);
            try {
                admin.close();
            } catch (IOException e1) {
                System.out.println("Error occurred while cloing the HBaseAdmin conneciton:" + e1);
            }
        } catch (MasterNotRunningException e) {
            throw new RuntimeException("HBase master not running, table creation failed.");
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException("Zookeeper not running, table creation failed.");
        } catch (IOException e) {
            throw new RuntimeException("IO error, table creation failed.");
        }
    }

    public void insert(Map<String, Map<String, Object>> record, String rowId) {
        try {
            Put put = new Put(Bytes.toBytes(rowId));
            for (String cf : record.keySet()) {
                for (String column : record.get(cf).keySet()) {
                    put.add(Bytes.toBytes(cf),
                            Bytes.toBytes(column),
                            Bytes.toBytes(record.get(cf).get(column).toString()));
                }
            }
            hTable.put(put);
        } catch (IOException e) {
            throw new RuntimeException("Error occurred while storing record into HBase");
        }
    }

    public static void main(String[] args) {
        List<String> cFs = new ArrayList<String>();

        cFs.add("cf1");
        cFs.add("cf2");

        List<String> zks = new ArrayList<String>();
        zks.add("127.0.0.1");

        Map<String, Map<String, Object>> record = new HashMap<String, Map<String, Object>>();

        Map<String, Object> cf1 = new HashMap<String, Object>();
        cf1.put("aa", "1");

        Map<String, Object> cf2 = new HashMap<String, Object>();
        cf2.put("bb", "1");

        record.put("cf1", cf1);
        record.put("cf2", cf2);

        HBaseOperations operations = new HBaseOperations("skyfree_big_table", cFs, zks, 2181);
        operations.insert(record, UUID.randomUUID().toString());
        System.out.println("done");
    }
}
