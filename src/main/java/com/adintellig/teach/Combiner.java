package com.adintellig.teach;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner extends Reducer<Text, Text, Text, Text> {

	Text v = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		for (Text value : values) {
			sum += Double.parseDouble(value.toString());
		}
		Text v = new Text(String.valueOf(sum));
		// v.set(String.valueOf(sum));
		context.write(key, v);
	}
}
