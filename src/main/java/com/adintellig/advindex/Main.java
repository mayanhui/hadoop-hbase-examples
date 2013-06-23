package com.adintellig.advindex;

// /data/dm/baofengindex/crowd_attrs/bf_index_vv_play_album_crowd_attrs.txt

// /data/dm/advindex/adv_hour_movie/
///data/dm/advindex/adv_hour_movie/20130101/00

// /data/dm/advindex/material_hour_movie
// /data/dm/advindex/material_hour_movie/20130101/00

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

import com.adintellig.util.DateFormatUtil;


import java.io.File;

public class Main {
	static final Log LOG = LogFactory.getLog(Main.class);

	public static final String NAME = "Crowd-Attribute";
	public static final String TMP_FILE_PATH = "/tmp/advindex";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		String output = cmd.getOptionValue("o");
		String input = cmd.getOptionValue("i");
		String date = cmd.getOptionValue("d");
		String hour = cmd.getOptionValue("h");
		String jobConf = cmd.getOptionValue("jobconf");
		String[] arr = null;
		if (null != jobConf && jobConf.length() > 0) {
			arr = jobConf.split("=", -1);
			if (arr.length == 2) {
				conf.set(arr[0], arr[1]);
			}
		} else {
			conf.set("mapred.job.queue.name", "ETL");
		}

		if (null == date || date.length() <= 0) {
			date = DateFormatUtil.parseToStringDate(System.currentTimeMillis());
		}

		if (null == hour || hour.length() <= 0) {
			hour = DateFormatUtil.getLastHour();
		}

		if (output.endsWith(java.io.File.separator)) {
			output = output + date;
		} else
			output = output + java.io.File.separator + date;

		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(output);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		Path tmpPath = new Path(TMP_FILE_PATH);
		if (fs.exists(tmpPath)) {
			fs.delete(tmpPath, true);
		}

		arr = input.split(",", -1);
		if (arr.length == 2) {
			String hourData = null;
			String attrData = null;
			if (arr[0].indexOf("advindex") >= 0) {
				hourData = arr[0].trim();
				attrData = arr[1].trim();
			} else {
				hourData = arr[1].trim();
				attrData = arr[0].trim();
			}
			input = generateHourDataInput(hourData, date, hour) + ","
					+ attrData;
		}

		System.out.println("Inputs: " + input);
		System.out.println("Output: " + output);
		System.out.println("TempOut: " + TMP_FILE_PATH);

		/* step1: adid-attr mapping */
		Job job = new Job(conf, "advindex adid-attr mapping");
		job.setJarByClass(Main.class);
		job.setMapperClass(AdidAttrMapper.class);
		job.setReducerClass(AdidAttrReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, tmpPath);

		int success = job.waitForCompletion(true) ? 0 : 1;

		if (success == 0) {
			/* step2: aggregation */
			job = new Job(conf, "advindex aggregation");
			job.setJarByClass(Main.class);
			job.setMapperClass(AggrMapper.class);
			job.setReducerClass(AggrReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job, tmpPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			success = job.waitForCompletion(true) ? 0 : 1;
		}

		// delete tmp dirctory
		if (fs.exists(tmpPath)) {
			fs.delete(tmpPath, true);
		}

	}

	private static String generateHourDataInput(String input, String date,
			String hour) {
		String rst = null;
		if (null != input && null != date && null != hour) {
			if (input.endsWith(File.separator)) {
				input = input.substring(0, input.length() - 1);
			}
			rst = input + java.io.File.separator + date
					+ java.io.File.separator + "00";
			int h = Integer.parseInt(hour);
			for (int i = 1; i < h; i++) {
				if (i < 10) {
					rst = rst + "," + input + java.io.File.separator + date
							+ java.io.File.separator + "0" + i;
				} else
					rst = rst + "," + input + java.io.File.separator + date
							+ java.io.File.separator + i;
			}
		}
		return rst;
	}

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("i", "input", true,
				"the directory or file to read from (must exist)");
		o.setArgName("input");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("o", "output", true, "output directory (must exist)");
		o.setArgName("output");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("d", "date", true,
				"the start date of data, such as: 20130101");
		o.setArgName("date");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("h", "hour", true, "hour, such as 01");
		o.setArgName("hour");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("jobconf", "hour", true, "jobconf");
		o.setArgName("jobconf");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("file", "hour", true, "file");
		o.setArgName("file");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("mapred_output_dir", "hour", true, "file");
		o.setArgName("mapred_output_dir");
		o.setRequired(false);
		options.addOption(o);

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(NAME + " ", options, true);
			System.exit(-1);
		}
		return cmd;
	}

}
