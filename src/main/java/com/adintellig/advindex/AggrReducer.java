package com.adintellig.advindex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.adintellig.util.DateFormatUtil;


public class AggrReducer extends Reducer<Text, Text, Text, Text> {

	private Text v = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if (key.toString().trim().length() <= 0)
			return;

		Map<String, Attribute> attrMap = new HashMap<String, Attribute>();
		Map<String, Attribute> perMap = new HashMap<String, Attribute>();

		for (Text v : values) {
			String[] attrs = v.toString().trim().split("\\s+", -1);

			if (attrs.length != 4)
				continue;

			String attr = attrs[0].trim();
			String type = attrs[1].trim();
			String width_value = attrs[2].trim();
			String percent_value = attrs[3].trim();

			String hashkey = attr + "\t" + type;
			double wvalue = Double.parseDouble(width_value);
			double pvalue = Double.parseDouble(percent_value);

			Attribute a = attrMap.get(hashkey);
			if (a == null) {
				a = new Attribute();
				a.add(wvalue);
				attrMap.put(hashkey, a);
			} else {
				a.add(wvalue);
			}

			a = perMap.get(hashkey);
			if (a == null) {
				a = new Attribute();
				a.add(pvalue);
				perMap.put(hashkey, a);
			} else {
				a.add(pvalue);
			}

		}

		Iterator<String> iter = attrMap.keySet().iterator();
		while (iter.hasNext()) {
			String hk = iter.next();
			Attribute attr1 = attrMap.get(hk);
			Attribute attr2 = perMap.get(hk);

			v.set(hk
					+ "\t"
					+ attr1.avg()
					+ "\t"
					+ attr2.avg()
					+ "\t"
					+ DateFormatUtil.parseToStringDate(System
							.currentTimeMillis()));
			context.write(key, v);
		}
	}
}
