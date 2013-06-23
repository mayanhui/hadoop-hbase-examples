package com.adintellig.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseManagerTest extends Thread {
	public Configuration config;
	public HTable table;
	public HBaseAdmin admin;

	ConfigProperties cp = ConfigFactory.getInstance().getConfigProperties(
			ConfigFactory.APP_CONFIG_PATH);

	public HBaseManagerTest() {
		config = HBaseConfiguration.create();
//		config.set("hbase.zookeeper.property.clientPort", "2181");

//		config.set("hbase.master", "192.168.85.210:60000");
//		config.set("hbase.zookeeper.quorum",
//				"data-test-210,data-test-211,data-test-212");
		
//		config.set("hbase.master", "192.168.205.38:60000");
		config.set("hbase.zookeeper.quorum",
				"data-test-210,data-test-211,yanhui-thinkpad-t430");
		
		try {
			table = new HTable(config,
					Bytes.toBytes("hbase_dqs_test"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		HBaseManagerTest m = new HBaseManagerTest();
		m.get("{0000171B-2E21-1583-2CD6-4D1134310182}", "cf1", "vvmid", 100);// 100 max records
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