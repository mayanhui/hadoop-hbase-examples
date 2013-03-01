package com.baofeng.advindex.user;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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

import com.baofeng.advindex.AdidAttrMapper;
import com.baofeng.advindex.AdidAttrReducer;
import com.baofeng.advindex.AggrMapper;
import com.baofeng.advindex.AggrReducer;
import com.baofeng.util.DateFormatUtil;
import com.hadoop.mapreduce.LzoTextInputFormat;

/**
 * User attribute forecast main class.
 * 
 * @author mayanhui
 * 
 */
public class UserAttributeForecastMain {
	static final Log LOG = LogFactory.getLog(UserAttributeForecastMain.class);

	public static final String NAME = "Crowd-Attribute";
	public static final String TMP_FILE_PATH = "/tmp/advindex_user_attr_forecast";
	public static final String CROWD_ATTR_MAPPING = "/data/dm/baofengindex/crowd_attrs/bf_index_vv_play_album_crowd_attrs.txt";
	public static final int NUM_REDUCE = 10 * 12;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		String output = cmd.getOptionValue("o");
		String input = cmd.getOptionValue("i");
		String date = cmd.getOptionValue("d");
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
		if (!date.matches("\\d+")) {
			throw new Exception("Input field 'data' should be '20130101'");
		}

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
			String vv = null;
			String mapping = null;
			if (arr[0].indexOf("vv") >= 0) {
				vv = arr[0].trim();
				mapping = arr[1].trim();
			} else {
				vv = arr[1].trim();
				mapping = arr[0].trim();
			}
			input = generateMonthInput(vv, date) + "," + mapping;
		}

		System.out.println("Inputs: " + input);
		System.out.println("Output: " + output);
		System.out.println("TempOut: " + TMP_FILE_PATH);

		/* step1: uid-mname distribution */
		Job job = new Job(conf, "advindex uid-mname distribution");
		job.setJarByClass(UserAttributeForecastMain.class);
		job.setMapperClass(VVMnameMappingMapper.class);
		job.setReducerClass(VVMnameMappingReducer.class);
		job.setNumReduceTasks(NUM_REDUCE);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		LzoTextInputFormat.setMaxInputSplitSize(job, 512 * 1024 * 1024L);
		LzoTextInputFormat.setMinInputSplitSize(job, 256 * 1024 * 1024L);

		LzoTextInputFormat.addInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(tmpPath, "1"));

		int success = job.waitForCompletion(true) ? 0 : 1;

		if (success == 0) {
			/* step2: adid-attr mapping */
			job = new Job(conf, "advindex adid-attr mapping");
			job.setJarByClass(UserAttributeForecastMain.class);
			job.setMapperClass(AdidAttrMapper.class);
			job.setReducerClass(AdidAttrReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job, new Path(tmpPath, "1"),
					new Path(CROWD_ATTR_MAPPING));
			FileOutputFormat.setOutputPath(job, new Path(tmpPath, "2"));

			success = job.waitForCompletion(true) ? 0 : 1;
		}

		if (success == 0) {
			/* step3: aggregation */
			job = new Job(conf, "advindex aggregation");
			job.setJarByClass(UserAttributeForecastMain.class);
			job.setMapperClass(AggrMapper.class);
			job.setReducerClass(AggrReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job, new Path(tmpPath, "2"));
			FileOutputFormat.setOutputPath(job, outputPath);

			success = job.waitForCompletion(true) ? 0 : 1;
		}
	}

	private static String generateMonthInput(String input, String date) {
		int m = 30;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < m; i++) {
			sb.append(input + File.separator + getDateByDay(date, i) + ",");
		}

		if (sb.length() > 0)
			sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	public static String getDateByDay(String dateStr, int day) {
		Calendar date = Calendar.getInstance();
		date.setTime(new Date(DateFormatUtil.formatStringTimeToLong2(dateStr)));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		date.add(Calendar.DATE, -day);
		return sdf.format(date.getTime());
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

		o = new Option("jobconf", "jobconf", true, "jobconf");
		o.setArgName("jobconf");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("file", "file", true, "file");
		o.setArgName("file");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("mapred_output_dir", "mapred_output_dir", true, "file");
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
