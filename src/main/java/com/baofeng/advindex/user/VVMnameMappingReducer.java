package com.baofeng.advindex.user;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.baofeng.advindex.Const;

public class VVMnameMappingReducer extends Reducer<Text, Text, Text, Text> {

	private Text k = new Text();
	private Text v = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<String> uids = new ArrayList<String>();
		List<String> mnames = new ArrayList<String>();

		for (Text v : values) {
			String vstr = v.toString();
			if (vstr.startsWith(Const.ATTR_PREFIX)) {
				vstr = vstr.substring(vstr.indexOf(Const.ATTR_PREFIX)
						+ Const.ATTR_PREFIX.length());
				mnames.add(vstr);
			} else if (vstr.startsWith(Const.ID_PREDIX)) {
				vstr = vstr.substring(vstr.indexOf(Const.ID_PREDIX)
						+ Const.ID_PREDIX.length());
				uids.add(vstr);
			}
		}

		for (String id : uids) {
			for (String mname : mnames) {
				k.set("0\t1\t" + id);
				v.set("3\t4\t5\t6\t" + mname);
				context.write(k, v);
			}
		}
	}
}
