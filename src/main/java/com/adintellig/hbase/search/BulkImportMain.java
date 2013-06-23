package com.adintellig.hbase.search;

/**
 *  Integrated MapReduce 读取hdfs上的文件.以HTable.put(put)的方式在map中完成数据写入，无reduce过程
 *  
 *  */

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.adintellig.util.ConfigFactory;
import com.adintellig.util.ConfigProperties;



public class BulkImportMain extends Configured implements Tool {
	static final Log LOG = LogFactory.getLog(BulkImportMain.class);

	ConfigProperties config = ConfigFactory.getInstance().getConfigProperties(
			ConfigFactory.APP_CONFIG_PATH);

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			LOG.info("Usage: 3 parameters needed!\nhadoop jar hbase-buld-import-0.1.jar <inputPath> <tableName> <columnfamily:column>");
			System.exit(1);
		}

		String input = args[0];
		String table = args[1];
		String columns = args[2];

		Configuration conf = HBaseConfiguration.create();
		// hbase master
		conf.set(ConfigProperties.CONFIG_NAME_HBASE_MASTER,
				config.getProperty(ConfigProperties.CONFIG_NAME_HBASE_MASTER));
		// zookeeper quorum
		conf.set(
				ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM,
				config.getProperty(ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM));

		Job job = new Job(conf, "Import from file " + input + " into table "
				+ table);
		job.setJarByClass(BulkImportMain.class);
		job.setMapperClass(SearchKeywordMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
		job.getConfiguration().set("conf.column", columns);

		FileInputFormat.addInputPath(job, new Path(input));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		int res = 1;
		try {
			res = ToolRunner.run(conf, new BulkImportMain(), otherArgs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(res);
	}

}