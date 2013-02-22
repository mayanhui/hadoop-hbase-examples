package com.baofeng.hbase.ads.recom;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UidAdidListReducer extends Reducer<Text, Text, Text, Text> {

	private Text v = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Set<String> adids = new HashSet<String>();
		for (Text v : values) {
			adids.add(v.toString());
		}
		
		StringBuilder sb = new StringBuilder();
		for (String adid : adids) {
			sb.append(adid + ",");
		}
		if (sb.length() > 0) {
			sb.setLength(sb.length() - 1);
		}
		
		v.set(sb.toString());
		context.write(key, v);
	}
}
