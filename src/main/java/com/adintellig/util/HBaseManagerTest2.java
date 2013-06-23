package com.adintellig.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseManagerTest2 extends Thread {
	public Configuration config;
	public HTable table;
	public HBaseAdmin admin;

	public HBaseManagerTest2() {
		config = HBaseConfiguration.create();
		config.set("hbase.master", "192.168.2.20:60000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum",
				"192.168.2.21,192.168.2.22,192.168.2.23");
		try {
			table = new HTable(config, Bytes.toBytes("kafka_test"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		HBaseManagerTest2 m = new HBaseManagerTest2();
		m.get("{0000171B-2E21-1583-2CD6-4D1134310182}", "bhvr", "vvmid", 100);// 100
																				// max
																				// records
		m.put();
	}

	public void put() throws IOException {
		long st = System.currentTimeMillis();
		Put put = null;

		put = new Put(Bytes.toBytes("row1"), 10L);// rowkey
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("mid"), // field one
				Bytes.toBytes(123111));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("stat_hour"), // field two
				Bytes.toBytes("20"));

		table.put(put); // put to server

		table.flushCommits();
		table.close(); // must close the client

		long en = System.currentTimeMillis();
		System.out.println("time: " + (en - st) + "... ms");
	}

	public void get(String rowkey, String columnFamily, String column,
			int versions) throws IOException {
		long st = System.currentTimeMillis();

		Get get = new Get(Bytes.toBytes(rowkey));
		get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

		Scan scanner = new Scan(get);
		scanner.setMaxVersions(versions);

		ResultScanner rsScanner = table.getScanner(scanner);

		for (Result result : rsScanner) {
			final List<KeyValue> list = result.list();
			for (final KeyValue kv : list) {
				System.out.println(Bytes.toStringBinary(kv.getValue()) + "\t"
						+ kv.getTimestamp()); // mid + time
			}

		}
		rsScanner.close();

		long en2 = System.currentTimeMillis();
		System.out.println("Total Time: " + (en2 - st) + " ms");
	}

}