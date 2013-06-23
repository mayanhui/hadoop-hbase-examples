package com.adintellig.teach;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Main {
	static final Log LOG = LogFactory.getLog(Main.class);

	public static final String NAME = "user-avg";
	public static final String INPUT = "/input/user.txt";
	public static final String OUTPUT = "/output";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(INPUT);
		Path outputPath = new Path(OUTPUT);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		Job job = new Job(conf, NAME);
		job.setJarByClass(Main.class);
		job.setMapperClass(UidMapper.class);
		job.setReducerClass(UidReducer.class);
		job.setCombinerClass(Combiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
//		job.setNumReduceTasks(2);
		
//		FileInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 512);
//		FileInputFormat.setMinInputSplitSize(job, 1024 * 1024 * 256);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		int success = job.waitForCompletion(true) ? 0 : 1;

		System.out.println(success);

	}

}
