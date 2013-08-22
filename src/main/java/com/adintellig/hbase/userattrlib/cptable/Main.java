package com.adintellig.hbase.userattrlib.cptable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import com.adintellig.util.ConfigFactory;
import com.adintellig.util.ConfigProperties;
import com.adintellig.util.Const;

public class Main {
	static final Log LOG = LogFactory.getLog(Main.class);

	public static final String NAME = "User Attribute Lib";
	public static String inputTable = "user_behavior_attribute_noregistered";
	public static String outputTable = "user_behavior_attribute_noregistered";
	public static String mapping = "/tmp/movieid-mapping.csv";

	static ConfigProperties config = ConfigFactory.getInstance()
			.getConfigProperties(ConfigFactory.APP_CONFIG_PATH);

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		Scan scan = new Scan();
		scan.setBatch(200);
		scan.setCaching(100);
		scan.setMaxVersions();

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
		job.setJarByClass(Main.class);

		TableMapReduceUtil.initTableMapperJob(inputTable, scan, Importer.class,
				null, null, job);

		TableMapReduceUtil.initTableReducerJob(outputTable, null, job, null,
				null, null, null);
		job.setNumReduceTasks(0);
		
		DistributedCache.addCacheFile(new Path(mapping).toUri(),
				job.getConfiguration());

		int success = job.waitForCompletion(true) ? 0 : 1;

		System.exit(success);
	}

}
