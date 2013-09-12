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
//import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class PaginationCounter {
	public Configuration config;
	public HTable table;
	public HBaseAdmin admin;

	public static final long MILLISECONDS_INTERVAL = 1000L * 3600L * 24L * 30L
			* 12L;// one year
	public static final String COMMON_SERARATOR = "\u0001";
	public static final byte[] POSTFIX = new byte[] { 1 };

	public PaginationCounter() {
		config = HBaseConfiguration.create();
		// config.set("hbase.master", "192.168.85.210:60000");
		// config.set("hbase.zookeeper.property.clientPort", "2181");
		// config.set("hbase.zookeeper.quorum",
		// "data-test-210,data-test-211,data-test-212");

		config.set("hbase.master", "hbase-master:60000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum",
				"hbase-regionserver-64,hbase-regionserver-63,hbase-regionserver-62");

		try {
			table = new HTable(config,
					Bytes.toBytes("user_behavior_attribute_noregistered_index"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		PaginationCounter m = new PaginationCounter();
		// m.incr();
		// int t = m.getTotal("attr:movt_category_战争_area_港台");
		// System.out.println(t);
		// System.out.println("-------------------------------");
		// List<String> rows = m.pageVersion("attr:movt_category_电视_area_新加坡",
		// 10,
		// 1356001608213L);
		// System.out.println(rows.size());
		// for (String row : rows)
		// System.out.println(row);
		m.addScanCounter(null);

	}

	public void addScanCounter(byte[] startRow) throws IOException {
		long st = System.currentTimeMillis();

		int total = 0;
		Scan scanner = new Scan();

		/* version */
		scanner.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("rk"));
		scanner.setMaxVersions();

		/* batch and caching */
		scanner.setBatch(0);
		scanner.setCaching(1);
		if (null != startRow)
			scanner.setStartRow(startRow);
		
		ResultScanner rsScanner = null;
		String rowkey = null;
		byte[] rk = null;
		byte[] cf = Bytes.toBytes("cf1");
		byte[] c = Bytes.toBytes("uid_cnt");
		try {
			rsScanner = table.getScanner(scanner);

			for (Result res : rsScanner) {
				System.out.println("Count: " + total);
				++total;
				// final List<KeyValue> list = res.list();
				// int vers = res.list().size();
				rowkey = getRealRowKey(res.list().get(0));
				// System.out.println("rowkey1: " + rowkey);

				rowkey = new String(Bytes.toBytesBinary(rowkey), "utf-8");
				System.out.println("rowkey2: " + rowkey);

				rk = Bytes.toBytes(rowkey);

				long cur = table.incrementColumnValue(rk, cf, c,
						Long.parseLong(String.valueOf(res.list().size())));
				System.out.println("cur: " + cur);
			}
		} catch (Exception e) {
			if(null != rowkey){
				addScanCounter(Bytes.toBytes(rowkey));
			}
		}

		rsScanner.close();

		long en = System.currentTimeMillis();
		System.out.println("Count: " + total);
		System.out.println("Total Time: " + (en - st) + " ms");

	}

	private static String getRealRowKey(KeyValue kv) {
		int rowlength = Bytes.toShort(kv.getBuffer(), kv.getOffset()
				+ KeyValue.ROW_OFFSET);
		String rowKey = Bytes.toStringBinary(kv.getBuffer(), kv.getOffset()
				+ KeyValue.ROW_OFFSET + Bytes.SIZEOF_SHORT, rowlength);
		return rowKey;
	}

	public void incr() throws IOException {
		long st = System.currentTimeMillis();
		byte[] rk = Bytes.toBytes("row1");
		byte[] cf = Bytes.toBytes("cf1");
		byte[] c = Bytes.toBytes("mid_cnt");

		long cur = table.incrementColumnValue(rk, cf, c, 0L);
		System.out.println("cur: " + cur);
		cur = table.incrementColumnValue(rk, cf, c, 1024L);
		System.out.println("cur: " + cur);
		cur = table.incrementColumnValue(rk, cf, c, 0L);
		System.out.println("cur: " + cur);

		long en = System.currentTimeMillis();

		System.out.println("time: " + (en - st) + "... ms");
	}

	public int getTotal(String rowkey) throws IOException {
		byte[] rk = Bytes.toBytes(rowkey);
		byte[] cf = Bytes.toBytes("cf1");
		byte[] c = Bytes.toBytes("uid_cnt");

		return (int) table.incrementColumnValue(rk, cf, c, 0L);
	}

	public void pageRow(int pageSize, byte[] lastRow) throws IOException {
		long st = System.currentTimeMillis();

//		Filter filter = new PageFilter(pageSize);
		Filter filter = new QualifierFilter(CompareOp.EQUAL,new RegexStringComparator("^M_.*"));
		
		int totalRows = 0;

		Scan scan = new Scan();
		scan.setFilter(filter);
		if (lastRow != null) {
			byte[] startRow = Bytes.add(lastRow, POSTFIX);
			System.out.println("start row: " + Bytes.toStringBinary(startRow));
			scan.setStartRow(startRow);
		}
		ResultScanner scanner = table.getScanner(scan);
		int localRows = 0;
		Result result;
		while ((result = scanner.next()) != null) {
			System.out.println(localRows++ + ": " + result);
			totalRows++;
			lastRow = result.getRow();
		}
		scanner.close();

		System.out.println("total rows: " + totalRows);

		long en2 = System.currentTimeMillis();
		System.out.println("Total Time: " + (en2 - st) + " ms");
	}

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