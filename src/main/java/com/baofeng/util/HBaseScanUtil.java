package com.baofeng.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseScanUtil {

	public static Configuration config;
	public static HTable table;
	public static HBaseAdmin admin;
	public static ConfigProperties cp = ConfigFactory.getInstance()
			.getConfigProperties(ConfigFactory.APP_CONFIG_PATH);

	static {
		config = HBaseConfiguration.create();
		config.set("hbase.master",
				cp.getProperty(ConfigProperties.CONFIG_NAME_HBASE_MASTER));
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set(
				"hbase.zookeeper.quorum",
				cp.getProperty(ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM));
		try {
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		// HBaseScanUtil.countByLimitVer("user_behavior_attribute_noregistered",
		// "bhvr", "vvmid", 500);
		// HBaseScanUtil.getRowkeyListByLimitVer(
		// "user_behavior_attribute_noregistered", "bhvr", "vvmid", 500);
		// HBaseScanUtil.getRowkeyColumnValueListByVer(
		// "user_behavior_attribute_noregistered", "bhvr", "vvmid", 50,
		// 500);
		
		if(args.length < 2){
			System.out.println("Input args must be 2!");
			return;
		}
		
		String start = args[0];
		String end = args[1]; //20130306

		HBaseScanUtil.getRowkeyColumnVersionCount(
				"user_behavior_attribute_noregistered", "bhvr", "vvmid",
				DateFormatUtil.formatStringTimeToLong2(start),
				DateFormatUtil.formatStringTimeToLong2(end), true);

	}

	/**
	 * 统计所有rowkey下某个字段的版本数量（或者去重版本数量）
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param column
	 * @param minStamp
	 * @param maxStamp
	 * @param distinct
	 * @throws IOException
	 */
	public static void getRowkeyColumnVersionCount(String tableName,
			String columnFamily, String column, long minStamp, long maxStamp,
			boolean distinct) throws IOException {
		int total = 0;
		long st = System.currentTimeMillis();
		table = new HTable(config, Bytes.toBytes(tableName));
		Scan scanner = new Scan();

		/* version */
		scanner.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		scanner.setTimeRange(minStamp, maxStamp);
		scanner.setMaxVersions();

		/* batch and caching */
		scanner.setBatch(0);
		scanner.setCaching(10000);

		ResultScanner rsScanner = table.getScanner(scanner);

		if (distinct) {
			for (Result res : rsScanner) {
				Set<String> set = new HashSet<String>();
				final List<KeyValue> list = res.list();
				String rk = null;
				for (final KeyValue kv : list) {
					set.add(Bytes.toStringBinary(kv.getValue()));
					rk = getRealRowKey(kv);
				}
				System.out.println(rk + "\t" + set.size());
			}
		} else {
			for (Result res : rsScanner) {
				List<String> vals = new ArrayList<String>();
				final List<KeyValue> list = res.list();
				String rk = null;
				for (final KeyValue kv : list) {
					vals.add(Bytes.toStringBinary(kv.getValue()));
					rk = getRealRowKey(kv);
				}
				System.out.println(rk + "\t" + vals.size());
			}
		}

		rsScanner.close();

		long en = System.currentTimeMillis();
		System.out.println("Count: " + total);
		System.out.println("Total Time: " + (en - st) + " ms");
	}

	/**
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param column
	 * @param limitVer
	 * @return
	 * @throws IOException
	 */
	public static void getRowkeyColumnValueListByVer(String tableName,
			String columnFamily, String column, int startVer, int endVer)
			throws IOException {
		int total = 0;
		long st = System.currentTimeMillis();
		table = new HTable(config, Bytes.toBytes(tableName));
		Scan scanner = new Scan();

		/* version */
		scanner.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		scanner.setMaxVersions();

		/* batch and caching */
		scanner.setBatch(0);
		scanner.setCaching(10000);

		ResultScanner rsScanner = table.getScanner(scanner);

		for (Result res : rsScanner) {
			final List<KeyValue> list = res.list();
			int vers = list.size();
			if (vers >= startVer) {
				Set<String> set = new HashSet<String>();
				++total;
				String rk = null;
				for (final KeyValue kv : list) {
					set.add(Bytes.toStringBinary(kv.getValue()));
					// System.out.println(getRealRowKey(kv));
					// break;
					rk = getRealRowKey(kv);
				}

				if (set.size() >= startVer && set.size() <= endVer) {
					StringBuilder sb = new StringBuilder();
					for (String s : set) {
						sb.append(s + ",");
					}
					if (sb.length() > 0)
						sb.setLength(sb.length() - 1);

					System.out.println(rk + "\t" + sb.toString());
				}

			}
		}

		rsScanner.close();

		long en = System.currentTimeMillis();
		System.out.println("Count: " + total);
		System.out.println("Total Time: " + (en - st) + " ms");

	}

	/**
	 * Get所有版本大于一个阈值的所有rowkey的list
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param column
	 * @param limitVer
	 * @return
	 * @throws IOException
	 */
	public static void getRowkeyListBiggerThanLimitVer(String tableName,
			String columnFamily, String column, int limitVer)
			throws IOException {
		int total = 0;
		long st = System.currentTimeMillis();
		table = new HTable(config, Bytes.toBytes(tableName));
		Scan scanner = new Scan();

		/* version */
		scanner.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		scanner.setMaxVersions();

		/* batch and caching */
		scanner.setBatch(0);
		scanner.setCaching(10000);

		ResultScanner rsScanner = table.getScanner(scanner);

		for (Result res : rsScanner) {
			final List<KeyValue> list = res.list();

			int vers = list.size();
			if (vers >= limitVer) {
				++total;
				for (final KeyValue kv : list) {
					System.out.println(getRealRowKey(kv));
					break;
				}
				// System.out.println(total + "...");
			}
		}

		rsScanner.close();

		long en = System.currentTimeMillis();
		System.out.println("Count: " + total);
		System.out.println("Total Time: " + (en - st) + " ms");

	}

	/**
	 * 查找所有版本大于一个阈值的所有rowkey的计数
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param column
	 * @param limitVer
	 * @return
	 * @throws IOException
	 */
	public static int countBiggerThanLimitVer(String tableName, String columnFamily,
			String column, int limitVer) throws IOException {
		int total = 0;
		long st = System.currentTimeMillis();
		table = new HTable(config, Bytes.toBytes(tableName));
		Scan scanner = new Scan();

		/* version */
		scanner.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		scanner.setMaxVersions();

		/* batch and caching */
		scanner.setBatch(0);
		scanner.setCaching(10000);

		ResultScanner rsScanner = table.getScanner(scanner);

		for (Result res : rsScanner) {
			final List<KeyValue> list = res.list();

			for (final KeyValue kv : list)
				System.out.println(kv.toString());

			int vers = list.size();
			if (vers >= limitVer) {
				++total;
				System.out.println(total + "...");
			}
		}

		rsScanner.close();

		long en = System.currentTimeMillis();
		System.out.println("Count: " + total);
		System.out.println("Total Time: " + (en - st) + " ms");

		return total;
	}

	static String getRealRowKey(KeyValue kv) {
		int rowlength = Bytes.toShort(kv.getBuffer(), kv.getOffset()
				+ KeyValue.ROW_OFFSET);
		String rowKey = Bytes.toStringBinary(kv.getBuffer(), kv.getOffset()
				+ KeyValue.ROW_OFFSET + Bytes.SIZEOF_SHORT, rowlength);
		return rowKey;
	}
}
