package com.baofeng.util;

//import java.util.Date;
//import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
//import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
//import org.apache.hadoop.hbase.filter.RegexStringComparator;
//import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseManager extends Thread {
	public Configuration config;
	public HTable table;
	public HBaseAdmin admin;

	ConfigProperties cp = ConfigFactory.getInstance().getConfigProperties(
			ConfigFactory.APP_CONFIG_PATH);

	public HBaseManager() {
		config = HBaseConfiguration.create();
		config.set("hbase.master",
				cp.getProperty(ConfigProperties.CONFIG_NAME_HBASE_MASTER));
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set(
				"hbase.zookeeper.quorum",
				cp.getProperty(ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM));
		try {
//			table = new HTable(config,
//					Bytes.toBytes("user_behavior_attribute_noregistered"));
			table = new HTable(config,
					Bytes.toBytes("demo_table"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					"/home/mayanhui/uids.txt")));
			long start = System.currentTimeMillis();
			String line = null;
			int count = 0;
			String[] columns = new String[] { "version", "type" };
			while (null != (line = br.readLine())) {
				++count;
				get(line.trim(), "vv_log", columns);
			}

			long end = System.currentTimeMillis();
			System.out.println("Total time: " + (end - start) + " ms.");
			System.out.println("Total count: " + count);
			System.out.println("AVG time: " + (end - start) / (double) count
					+ " ms.");
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		HBaseManager m = new HBaseManager();
		// m.testScanner();
		// m.put();
		 m.testGet();
		// m.testScanGet();
//		m.testPageFilter();
		System.out.println("-------------------------------");
//		m.testColumnPaginationFilter();
//		Put put = new Put(Bytes.toBytes("{1F591795-74DE-EB70-0245-0E4465C72CFA}"));
//		put.add(Bytes.toBytes("bhvr"), Bytes.toBytes("vvmid"),
//				Bytes.toBytes(123111));
//		System.out.println(put.toJSON());

		
	}

	public static final boolean AUTO_FLUSH = false;
	public static final int WRITE_BUFFER_SIZE = 12 * 1024 * 1024;

	public void put() throws IOException {
		// table.setAutoFlush(AUTO_FLUSH);
		// table.setWriteBufferSize(WRITE_BUFFER_SIZE);

		long st = System.currentTimeMillis();
		Put put = null;

		// for (int i = 0; i < 100000; i++) {

		put = new Put(Bytes.toBytes("{1F591795-74DE-EB70-0245-0E4465C72CFA}"));
		put.add(Bytes.toBytes("bhvr"), Bytes.toBytes("vvmid"),
				Bytes.toBytes(123111));
		// put.add(Bytes.toBytes("vv_log"), Bytes.toBytes("stat_hour"),
		// Bytes.toBytes("20"));
		// put.add(Bytes.toBytes("vv_log"), Bytes.toBytes("logdate"),
		// Bytes.toBytes("20121126"));
		// put.add(Bytes.toBytes("vv_log"), Bytes.toBytes("ditch"),
		// Bytes.toBytes("2"));
		// put.add(Bytes.toBytes("vv_log"), Bytes.toBytes("version"),
		// Bytes.toBytes("3.2.2.2"));
		// put.add(Bytes.toBytes("vv_log"), Bytes.toBytes("type"),
		// Bytes.toBytes("2"));
		// put.add(Bytes.toBytes("vv_log"), Bytes.toBytes("product"),
		// Bytes.toBytes("baofeng.com"));
		// put.setWriteToWAL(true);

		table.put(put);

		// if ((i % 1000) == 0) {
		// System.out.println(i + " DOCUMENTS done!");
		// }
		// }
		

		table.flushCommits();
		table.close();

		long en = System.currentTimeMillis();
		System.out.println("time: " + (en - st) + "... ms");
	}

	public void get(String rowkey, String columnFamily, String[] columns)
			throws IOException {
		Get get = new Get(Bytes.toBytes(rowkey));
		for (int i = 0; i < columns.length; i++) {
			get.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(columns[i]));
		}
		Result dbResult = table.get(get);
		try {
			System.out.println("size=" + dbResult.size() + ", value="
					+ Bytes.toString(dbResult.list().get(0).getValue()));
		} catch (Exception e) {
		}
		
		

	}

	public void testQueryRS() throws Exception {

		Scan scanner = new Scan();
		scanner.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("description"));
		scanner.setMaxVersions();
		ResultScanner rsScanner = table.getScanner(scanner);
		System.out.println(rsScanner.toString());
		Result rs = rsScanner.next();
		int count = 0;
		while (null != rs) {
			++count;
			System.out.println(rs.size());
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> nMap = rs
					.getMap();
			NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = nMap
					.get(Bytes.toBytes("description"));
			NavigableMap<Long, byte[]> qualMap = columnMap.get(new byte[] {});

			if (qualMap.entrySet().size() > 0) {
				System.out.println("---------------------------");
				for (Map.Entry<Long, byte[]> m : qualMap.entrySet()) {
					System.out.println("Value:" + new String(m.getValue()));
				}
			}
			rs = rsScanner.next();
			if (count > 10)
				break;
		}
	}

	public void testQueryCommodity() throws Exception {

		System.out.println("Get Spin's commodity info");
		Get mathGet = new Get(new String("Spin").getBytes());
		mathGet.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("widgetname"));
		mathGet.setMaxVersions();
		Result rs = table.get(mathGet);

		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> nMap = rs
				.getMap();
		NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = nMap
				.get(Bytes.toBytes("widgetname"));
		NavigableMap<Long, byte[]> qualMap = columnMap.get(new byte[] {});

		if (qualMap.entrySet().size() > 0) {
			for (Map.Entry<Long, byte[]> m : qualMap.entrySet()) {
				System.out.println("Value:" + new String(m.getValue()));
				break;
			}
		}
	}

	public void test() throws Exception {

		if (admin.tableExists("scores")) {
			System.out.println("drop table");
			admin.disableTable("scores");
			admin.deleteTable("scores");
		}

		System.out.println("create table");
		HTableDescriptor tableDescripter = new HTableDescriptor(
				"scores".getBytes());
		tableDescripter.addFamily(new HColumnDescriptor("grade"));
		tableDescripter.addFamily(new HColumnDescriptor("course"));

		admin.createTable(tableDescripter);

		System.out.println("add Tom's data");

		Put tomPut = new Put(new String("Tom").getBytes());
		tomPut.add(new String("grade").getBytes(), new byte[] {}, new String(
				"1").getBytes());
		tomPut.add(new String("grade").getBytes(),
				new String("math").getBytes(), new String("87").getBytes());
		tomPut.add(new String("course").getBytes(),
				new String("math").getBytes(), new String("97").getBytes());
		table.put(tomPut);

		System.out.println("add Jerry's data");

		Put jerryPut = new Put(new String("Jerry").getBytes());
		jerryPut.add(new String("grade").getBytes(), new byte[] {}, new String(
				"2").getBytes());
		jerryPut.add(new String("grade").getBytes(),
				new String("math").getBytes(), new String("77").getBytes());
		jerryPut.add(new String("course").getBytes(),
				new String("math").getBytes(), new String("92").getBytes());
		table.put(jerryPut);

		System.out.println("Get Tom's data");
		Get tomGet = new Get(new String("Tom").getBytes());
		Result tomResult = table.get(tomGet);
		System.out.println(tomResult.toString());

		System.out.println("Get Tom's Math grade");
		Get mathGet = new Get(new String("Tom").getBytes());
		mathGet.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("grade"));
		mathGet.setMaxVersions();
		Result rs = table.get(mathGet);

		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> nMap = rs
				.getMap();
		NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = nMap
				.get(Bytes.toBytes("grade"));
		NavigableMap<Long, byte[]> qualMap = columnMap.get(Bytes
				.toBytes("math"));

		for (Map.Entry<Long, byte[]> m : qualMap.entrySet()) {

			System.out.println("TimeStamp:" + m.getKey());
			System.out.println("Value:" + new String(m.getValue()));
		}

		System.out.println("Delete a column");
		Delete deleteArt = new Delete(Bytes.toBytes("Tom"));
		deleteArt.deleteColumn(Bytes.toBytes("grade"), Bytes.toBytes("math"));
		table.delete(deleteArt);
	}

	public void testScanner() throws IOException {
		long st = System.currentTimeMillis();
		// System.out.println("Scan....");

		Scan scanner = new Scan();

		// /*version*/
		// scanner.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("mid"));
		// scanner.setTimeRange(1253997917140L, 1453997917140L);
		scanner.setMaxVersions();

		/* filter */
		// Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new
		// RegexStringComparator("20121.*_AF6E39E3-3174-7BBF-7EE9-02AB6528897B"));
		// scanner.setFilter(filter);

		/* batch and caching */
		// scanner.setBatch(0);
		// scanner.setCaching(100000);

		ResultScanner rsScanner = table.getScanner(scanner);

		// long en = System.currentTimeMillis();
		// System.out.println("search Time: " + (en - st) + " ms");

		int version = 0;
		for (Result res : rsScanner) {
			version++;
			final List<KeyValue> list = res.list();
			// System.out.println(getRealRowKey(list.get(0)));
			for (final KeyValue kv : list) {
				kv.getTimestamp();
				System.out.println(getRealRowKey(kv));
				break;
			}

			for (final KeyValue kv : list)
				System.out.println(kv.toString());

			version = list.size();
			// System.out.println("Time: " + System.currentTimeMillis());
			// if(version % 10000 == 0)
			// System.out.println("rows: " + version);
		}

		rsScanner.close();

		long en2 = System.currentTimeMillis();
		System.out.println("Total rows: " + version);
		System.out.println("Total Time: " + (en2 - st) + " ms");

		// long en = System.currentTimeMillis();
		// System.out.println("Time: " + (en-st) +" ms");
		//
		// Result rs = rsScanner.next();
		// for (; null != rs; rs = rsScanner.next()) {
		// System.out.println("rs.getRow()[" + new String(rs.getRow()) + "]");
		// System.out.println("["
		// + new String(rs.getValue(Bytes.toBytes("vv_log"),
		// Bytes.toBytes("aid"))) + "]");
		//
		// }
	}

	public static String getRealRowKey(KeyValue kv) {
		int rowlength = Bytes.toShort(kv.getBuffer(), kv.getOffset()
				+ KeyValue.ROW_OFFSET);
		String rowKey = Bytes.toStringBinary(kv.getBuffer(), kv.getOffset()
				+ KeyValue.ROW_OFFSET + Bytes.SIZEOF_SHORT, rowlength);
		return rowKey;
	}

	public void testGet() throws IOException {
		long st = System.currentTimeMillis();
		Get get = new Get(
				Bytes.toBytes("i2"));
		get.addColumn(Bytes.toBytes("msg"), Bytes.toBytes("title"));
		get.setMaxVersions(100);
		// get.setTimeRange(1354010844711L - 12000L, 1354010844711L);

		get.setTimeStamp(3L);

//		Filter filter = new ColumnPaginationFilter(1, 10);
//		get.setFilter(filter);

		Result dbResult = table.get(get);

		System.out.println("result=" + dbResult.toString());
		System.out.println("result=" + dbResult.list().get(0).getTimestamp());
		long en2 = System.currentTimeMillis();
		System.out.println("Total Time: " + (en2 - st) + " ms");

	}

	public void testScanGet() throws IOException {
		long st = System.currentTimeMillis();

		Get get = new Get(
				Bytes.toBytes("{00CB0E1D-2232-5D72-658D-140058DE1B83}"));
		get.addColumn(Bytes.toBytes("bhvr"), Bytes.toBytes("vvmid"));

		Scan scanner = new Scan(get);
		scanner.setMaxVersions(100);
		// scanner.setCaching(100);
		// scanner.setBatch(10);
		
		ResultScanner rsScanner = table.getScanner(scanner);

		for (Result result : rsScanner) {
			final List<KeyValue> list = result.list();
			for (final KeyValue kv : list) {
				System.out.println(kv.toString());
				// System.out.println(getRealRowKey(kv));
				System.out.println(kv.getTimestamp());
				// System.out.println(kv.getTimestampOffset());
				// System.out.println(KeyValue.TIMESTAMP_SIZE);

				System.out.println();
			}

		}
		rsScanner.close();

		long en2 = System.currentTimeMillis();
		System.out.println("Total Time: " + (en2 - st) + " ms");
	}

	public static final byte[] POSTFIX = new byte []{1};

	public void testPageFilter() throws IOException {
		long st = System.currentTimeMillis();

		Filter filter = new PageFilter(15);
		int totalRows = 0;
		byte[] lastRow = null;
		while (true) {
			Scan scan = new Scan();
			scan.setFilter(filter);
			if (lastRow != null) {
				byte[] startRow = Bytes.add(lastRow, POSTFIX);
				System.out.println("start row: "
						+ Bytes.toStringBinary(startRow));
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
			if (localRows == 0)
				break;
		}
		System.out.println("total rows: " + totalRows);

		long en2 = System.currentTimeMillis();
		System.out.println("Total Time: " + (en2 - st) + " ms");
	}

	public void testColumnPaginationFilter() throws IOException {
		long st = System.currentTimeMillis();

		Filter filter = new ColumnPaginationFilter(5, 2);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(result);
		}
		scanner.close();

		long en2 = System.currentTimeMillis();
		System.out.println("Total Time: " + (en2 - st) + " ms");
	}

}