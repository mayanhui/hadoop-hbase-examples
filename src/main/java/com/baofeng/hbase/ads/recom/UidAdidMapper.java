package com.baofeng.hbase.ads.recom;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UidAdidMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String arr[] = valueStr.split("\t", -1);
		if (arr.length == 2) {
			k.set(arr[1].trim());// tag-id
			v.set(arr[0].trim());// uid || adid
			context.write(k, v);
		}
	}
}
