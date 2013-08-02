package com.adintellig;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.HConstants;

import com.adintellig.util.ConfigProperties;

public class Test2 {
	public static void main(String[] args) throws IOException {
//		String[] stats = ZKUtil.getServerStats("114.112.82.21:2181", 1000);
//		for (String s : stats) {
//			System.out.println(s);
//		}
//		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.master", "114.112.82.20:60000");
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
//		conf.set("hbase.zookeeper.quorum",
//				"114.112.82.23,114.112.82.22,114.112.82.21");
//		try {
//			HMaster master = new HMaster(conf);
//			ZooKeeperWatcher watcher = master.getZooKeeperWatcher();
//			String dumpStr = ZKUtil.dump(watcher);
//			System.out.println(dumpStr);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
		long start = System.currentTimeMillis() - 30 * 24 * 3600 * 1000L;
		long end = System.currentTimeMillis();
		System.out.println(start);
		System.out.println(end);
		
		

	}
}
