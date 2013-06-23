package com.adintellig.advindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdidAttrReducer extends Reducer<Text, Text, Text, Text> {

	private Text k = new Text();
	private Text v = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<String> ids = new ArrayList<String>();
		List<String> attrs = new ArrayList<String>();

		for (Text v : values) {
			String vstr = v.toString();
			if (vstr.startsWith(Const.ATTR_PREFIX)) {
				vstr = vstr.substring(vstr.indexOf(Const.ATTR_PREFIX)
						+ Const.ATTR_PREFIX.length());
				attrs.add(vstr);
			} else if (vstr.startsWith(Const.ID_PREDIX)) {
				vstr = vstr.substring(vstr.indexOf(Const.ID_PREDIX)
						+ Const.ID_PREDIX.length());
				ids.add(vstr);
			}
		}

		for (String id : ids) {
			for (String attr : attrs) {
				k.set(id);
				v.set(attr);
				context.write(k, v);
			}
		}
	}
	
}
