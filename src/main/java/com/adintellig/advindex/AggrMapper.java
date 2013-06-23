package com.adintellig.advindex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggrMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split("\t", -1);
		if (arr.length == 2) {
			k.set(arr[0]);
			v.set(arr[1]);
			context.write(k, v);
		}
	}
}
