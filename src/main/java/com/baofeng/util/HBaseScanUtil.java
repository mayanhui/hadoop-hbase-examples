package com.baofeng.util;

import java.io.IOException;
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
			.getConfigProperties(ConfigFactory.BULK_IMPORT_CONFIG_PATH);

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
//		HBaseScanUtil.getRowkeyListByLimitVer(
//				"user_behavior_attribute_noregistered", "bhvr", "vvmid", 500);
		HBaseScanUtil.getRowkeyColumnValueListByVer("user_behavior_attribute_noregistered", "bhvr", "vvmid", 50, 500);
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
//					System.out.println(getRealRowKey(kv));
//					break;
					rk = getRealRowKey(kv);
				}
				
				if(set.size() >= startVer && set.size() <= endVer){
					StringBuilder sb = new StringBuilder();
					for(String s : set){
						sb.append(s + ",");
					}
					if(sb.length() > 0)
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
	public static void getRowkeyListByLimitVer(String tableName,
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
	public static int countByLimitVer(String tableName, String columnFamily,
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
