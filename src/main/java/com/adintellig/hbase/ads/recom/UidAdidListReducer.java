package com.adintellig.hbase.ads.recom;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UidAdidListReducer extends Reducer<Text, Text, Text, Text> {
	private Text v = new Text();

	public void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Set<String> adids = new HashSet<String>();
		for (Text v : values) {
			adids.add(v.toString());
		}

		StringBuilder sb = new StringBuilder();
		for (String adid : adids) {
			sb.append(new StringBuilder().append(adid).append(",").toString());
		}
		if (sb.length() > 0) {
			sb.setLength(sb.length() - 1);
		}

		this.v.set(sb.toString());
		context.write(key, this.v);
	}
}