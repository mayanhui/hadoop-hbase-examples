package com.baofeng.hbase.ads.user;

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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.baofeng.util.ConfigFactory;
import com.baofeng.util.ConfigProperties;
import com.baofeng.util.DateFormatUtil;

public class Main {
	static final Log LOG = LogFactory.getLog(Main.class);

	public static final String NAME = "ADID-Recom-Compute";
	public static final String TMP_FILE_PATH_1 = "/tmp/attribute_ads_age_gender_1";
	// public static final String TMP_FILE_PATH_2 = "/tmp/attribute_ads_2";

	public static final String OUPUT_COLUMN = "attr:adid";

	static ConfigProperties config = ConfigFactory.getInstance()
			.getConfigProperties(ConfigFactory.APP_CONFIG_PATH);

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		String inputTable = cmd.getOptionValue("i");
		String field = cmd.getOptionValue("f");
		String column = cmd.getOptionValue("c");
		conf.set("conf.field", field);
		if (column.indexOf(":") < 0 && column.indexOf(",") < 0)
			throw new Exception(
					"Column is not invalid! such as: family1:qualifier1,family2:qualifier2");

		String startDateStr = cmd.getOptionValue("s");
		long startDate = -1L;
		if (null != startDateStr && startDateStr.length() > 0) {
			if (startDateStr.length() == 8) {
				startDate = DateFormatUtil
						.formatStringTimeToLong2(startDateStr);
			} else
				throw new Exception(
						"start-date format is invalid. must be 20130102");
		}

		String endDateStr = cmd.getOptionValue("e");
		long endDate = System.currentTimeMillis();
		if (null != endDateStr && endDateStr.length() > 0) {
			if (endDateStr.length() == 8) {
				endDate = DateFormatUtil.formatStringTimeToLong2(endDateStr);
			} else
				throw new Exception(
						"end-date format is invalid. must be 20130102");
		}

		Scan scan = new Scan();
		if (column != null) {
			String[] arr = column.split(",", -1);
			if (null != arr && arr.length > 0) {
				for (String c : arr) {
					byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(c));
					if (colkey.length > 1) {
						scan.addColumn(colkey[0], colkey[1]);
					} else {
						scan.addFamily(colkey[0]);
					}
				}
			}
			scan.setTimeRange(startDate, endDate);
			// scan.setMaxVersions(Integer.MAX_VALUE);
		}

		// hbase master
		conf.set(ConfigProperties.CONFIG_NAME_HBASE_MASTER,
				config.getProperty(ConfigProperties.CONFIG_NAME_HBASE_MASTER));
		// zookeeper quorum
		conf.set(
				ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM,
				config.getProperty(ConfigProperties.CONFIG_NAME_HBASE_ZOOKEEPER_QUORUM));

		conf.set("mapred.job.queue.name", "ETL");

		FileSystem fs = FileSystem.get(conf);
		Path tmpPath1 = new Path(TMP_FILE_PATH_1);
		if (fs.exists(tmpPath1)) {
			fs.delete(tmpPath1, true);
		}

		/* step-1: scan hbase */
		Job job = new Job(conf, "attribute_ads_redis_1");
		job.setJarByClass(Main.class);
		TableMapReduceUtil.initTableMapperJob(inputTable, scan,
				ScanUserAttrMapper.class, Text.class, Text.class, job);
		job.setJarByClass(Main.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, tmpPath1);

		int success = job.waitForCompletion(true) ? 0 : 1;

		/* step2: load redis */
		if (success == 0) {
			job = new Job(conf, "attribute_ads_redis_2");
			job.setJarByClass(Main.class);
			job.setMapperClass(RedisLoaderMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, tmpPath1);

			success = job.waitForCompletion(true) ? 0 : 1;
		}

	}

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("i", "input", true,
				"the directory or file to read from (must exist)");
		o.setArgName("input-table-name");
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
			formatter.printHelp(NAME + " ", options, true);
			System.exit(-1);
		}
		return cmd;
	}

}
