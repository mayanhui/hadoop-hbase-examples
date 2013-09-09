package com.adintellig.hbase.ads.user.requirement20130909;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.adintellig.util.ConfigFactory;
import com.adintellig.util.ConfigProperties;

import com.adintellig.hbase.ads.user.RedisLoaderMapper;

public class Main {
	static final Log LOG = LogFactory.getLog(Main.class);
	public static final String NAME = "ADID-Recom-Compute";
	public static final String TMP_FILE_PATH_1 = "/tmp/attribute_ads_age_gender_1";
	public static final String TMP_FILE_PATH_2 = "/tmp/attribute_ads_age_gender_2";
	public static final String OUPUT_COLUMN = "attr:adid";
	static ConfigProperties config = ConfigFactory.getInstance()
			.getConfigProperties("/app.properties");

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		CommandLine cmd = parseArgs(otherArgs);
		String input = cmd.getOptionValue("i");
		String field = cmd.getOptionValue("f");
		String column = cmd.getOptionValue("c");
		conf.set("conf.field", field);
		conf.set("mapred.job.queue.name", "ETL");
		if ((column.indexOf(":") < 0) && (column.indexOf(",") < 0)) {
			throw new Exception(
					"Column is not invalid! such as: family1:qualifier1,family2:qualifier2");
		}

		FileSystem fs = FileSystem.get(conf);
		Path tmpPath1 = new Path(TMP_FILE_PATH_1);
		if (fs.exists(tmpPath1)) {
			fs.delete(tmpPath1, true);
		}
		Path tmpPath2 = new Path(TMP_FILE_PATH_2);
		if (fs.exists(tmpPath2)) {
			fs.delete(tmpPath2, true);
		}

		Job job = new Job(conf, "attribute_ads_redis_2");
		job.setJarByClass(Main.class);
		job.setMapperClass(RedisLoaderMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, tmpPath2);

		int success = job.waitForCompletion(true) ? 0 : 1;
	}

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("i", "input", true,
				"the directory or file to read from (must exist)");

		o.setArgName("input");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("c", "column", true,
				"column to store row data into (must exist)");

		o.setArgName("family:qualifier");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("f", "field", true, "field to load redis");
		o.setArgName("field");
		o.setRequired(true);
		options.addOption(o);

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("ADID-Recom-Compute ", options, true);
			System.exit(-1);
		}
		return cmd;
	}
}