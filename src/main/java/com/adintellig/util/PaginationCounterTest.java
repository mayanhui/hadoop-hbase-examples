package com.adintellig.util;

import java.io.IOException;
import java.util.ArrayList;
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

public class PaginationCounterTest {
	public Configuration config;
	public HTable table;
	public HBaseAdmin admin;

	public static final long MILLISECONDS_INTERVAL = 1000L * 3600L * 24L * 30L
			* 12L;// one year
	public static final String COMMON_SERARATOR = "\u0001";
	public static final byte[] POSTFIX = new byte[] { 1 };

	public PaginationCounterTest() {
		config = HBaseConfiguration.create();
		config.set("hbase.master", "192.168.85.210:60000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum",
				"data-test-210,data-test-211,data-test-212");
		try {
			table = new HTable(config,
					Bytes.toBytes("user_behavior_attribute_noregistered_index"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		PaginationCounterTest m = new PaginationCounterTest();
		int t = m.getTotal("attr:movt_category_战争_area_港台");
		System.out.println(t);
		System.out.println("-------------------------------");
		List<String> rows = m.pageVersion("attr:movt_category_电视_area_新加坡", 10,
				1356001608213L);
		System.out.println(rows.size());
		for (String row : rows)
			System.out.println(row);
	}

	public int getTotal(String rowkey) throws IOException {
		byte[] rk = Bytes.toBytes(rowkey);
		byte[] cf = Bytes.toBytes("cf1");
		byte[] c = Bytes.toBytes("uid_cnt");

		return (int) table.incrementColumnValue(rk, cf, c, 0L);
	}

	/**
	 * 
	 * @param rowkey
	 * @param pageSize
	 * @param lastTimestamp  第一页，ts=当前时间
	 * @return
	 * @throws IOException
	 */
	public List<String> pageVersion(String rowkey, int pageSize,
			long lastTimestamp) throws IOException {
		long st = System.currentTimeMillis();
		List<String> rows = new ArrayList<String>();

		Get get = new Get(Bytes.toBytes(rowkey));
		get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("rk"));

		get.setTimeRange(lastTimestamp - MILLISECONDS_INTERVAL,
				lastTimestamp - 1);

		Scan scan = new Scan(get);
		scan.setMaxVersions();

		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			int row = 0;
			final List<KeyValue> list = res.list();
			for (final KeyValue kv : list) {
				row = row + 1;
				if (row > pageSize)
					break;
				rows.add(Bytes.toStringBinary(kv.getValue()) + COMMON_SERARATOR
						+ kv.getTimestamp());
			}
		}

		scanner.close();

		long en2 = System.currentTimeMillis();
		System.out.println("Total Time: " + (en2 - st) + " ms");

		return rows;
	}

}