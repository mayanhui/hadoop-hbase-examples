package com.adintellig.teach;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//uid score

public class UidMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text k = new Text();
	private Text v = new Text();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String uid = null;
		String score = null;

		String arr[] = valueStr.split("\t", -1);
		if (arr.length == 2) {
			System.out.println(valueStr);

			uid = arr[0].trim();
			score = arr[1].trim();

			k.set(uid);
			v.set(score);
			context.write(k, v);
//			context.write(new Text(uid), v);
			context.getCounter("job1", "record").increment(1);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}
