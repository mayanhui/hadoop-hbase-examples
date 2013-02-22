package com.baofeng.hbase.ads.recom;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.baofeng.util.Const;

public class UidAdidReducer extends Reducer<Text, Text, Text, Text> {

	private Text k = new Text();
	private Text v = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Set<String> adids = new HashSet<String>();
		Set<String> uids = new HashSet<String>();

		for (Text v : values) {
			String vstr = v.toString();
			if (vstr.startsWith(Const.HBASE_ADS_RECOM_UID)) {
				vstr = vstr.substring(vstr.indexOf(Const.HBASE_ADS_RECOM_UID)
						+ Const.HBASE_ADS_RECOM_UID.length());
				uids.add(vstr);
			} else {
				adids.add(vstr);
			}
		}

		for (String uid : uids) {
			for (String adid : adids) {
				k.set(uid);
				v.set(adid);
				context.write(k, v);
			}
		}
	}

	public static void main(String[] args) {
	}
}
