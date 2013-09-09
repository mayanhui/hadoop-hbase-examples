package com.adintellig.hbase.ads.recom;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UidAdidReducer extends Reducer<Text, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();

	public void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Set<String> adids = new HashSet<String>();
		Set<String> uids = new HashSet<String>();

		for (Text v : values) {
			String vstr = v.toString();
			if (vstr.startsWith("UID_")) {
				vstr = vstr.substring(vstr.indexOf("UID_") + "UID_".length());

				uids.add(vstr);
			} else {
				adids.add(vstr);
			}
		}

		for (Iterator<String> i$ = uids.iterator(); i$.hasNext();) {
			String uid = i$.next();
			for (String adid : adids) {
				this.k.set(uid);
				this.v.set(adid);
				context.write(this.k, this.v);
			}
		}
	}

	public static void main(String[] args) {
	}
}