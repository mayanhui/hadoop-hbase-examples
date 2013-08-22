package com.adintellig.hbase.userattrlib.cptable4;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import com.adintellig.util.ConfigFactory;
import com.adintellig.util.ConfigProperties;
import com.adintellig.util.Const;

public class Main2 {
	static final Log LOG = LogFactory.getLog(Main2.class);

	public static final String NAME = "User Attribute Lib";
	public static String inputTable = "user_behavior_attribute_noregistered";
	public static String outputTable = "user_test";
	public static String mapping = "/tmp/movieid-mapping.csv";

	static ConfigProperties config = ConfigFactory.getInstance()
			.getConfigProperties(ConfigFactory.APP_CONFIG_PATH);

	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.out.println("Please input 2 parameters: starttime endtime, like: 20130101 20130131");
			return;
		}
		
		long minStamp = Long.parseLong(args[0]);
		long maxStamp = Long.parseLong(args[1]);
		
		Configuration conf = HBaseConfiguration.create();

		Scan scan = new Scan();
		scan.setBatch(100);
		scan.setCaching(100);
		//scan.setTimeRange(minStamp, maxStamp);
		scan.setMaxVersions();
		
		// scan.setRaw(true);

		// hbase master
		conf.set(ConfigProperties.CONFIG_NAME_HBASE_MASTER,
				config.getProperty(ConfigProperties.CONFIG_NAME_HBASE_MASTER));
		// zookeeper quorum
		conf.set(
				ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM,
				config.getProperty(ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM));

		// set hadoop speculative execution to false
		conf.setBoolean(Const.HADOOP_MAP_SPECULATIVE_EXECUTION, false);
		conf.setBoolean(Const.HADOOP_REDUCE_SPECULATIVE_EXECUTION, false);

		Job job = new Job(conf, NAME + ": Copy " + inputTable + " to "
				+ outputTable);
		job.setJarByClass(Main2.class);

		TableMapReduceUtil.initTableMapperJob(inputTable, scan, ImporterExcludeVV.class,
				ImmutableBytesWritable.class, Put.class, job);
		job.setNumReduceTasks(1000);
		TableMapReduceUtil.initTableReducerJob(outputTable,
				IdentityTableReducer.class, job,HRegionPartitioner.class);
		
		
//		DistributedCache.addCacheFile(new Path(mapping).toUri(),
//				job.getConfiguration());

		int success = job.waitForCompletion(true) ? 0 : 1;

		System.exit(success);
	}

}
