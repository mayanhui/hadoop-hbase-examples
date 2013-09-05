package com.adintellig.hive.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import com.hadoop.mapred.DeprecatedLzoTextInputFormat;

/**
 * 
 * Must use old package of MapReduce: org.apache.hadoop.mapred.*.
 */
public class Main {

	public static final String input = "/data/dw/vv/20130728";
	public static final String output = "/data/dw/vv/20130728_orc";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name", "ETL");

		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(output);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		JobConf job = new JobConf(conf);
		job.setJarByClass(Main.class);
		job.setMapperClass(LZOORCMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setInputFormat(DeprecatedLzoTextInputFormat.class);
		job.setOutputFormat(OrcOutputFormat.class);

		DeprecatedLzoTextInputFormat.addInputPath(job, new Path(input));
		OrcOutputFormat.setOutputPath(job, new Path(output));
		
		JobClient.runJob(job);
	}
}
