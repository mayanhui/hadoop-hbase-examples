package com.baofeng.advindex.user;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.baofeng.advindex.Const;

//1,55375,13,,电影,非诚勿扰
//3,61342,13,6425002,电影,撕裂的末日
//4,21730,13,0,电影,双面疑杀
//6,29629,13,4,电影,邪恶新世界
//8,67808,13,4,电影,鸡仔总动员

//20130101	05	120.69.65.128	2013-01-01 05:00:01	GET	/vvlog.html	{644DAE61-7F61-15C3-0E92-0A00E2FFF823}	
//tree	36636	13	454127	1	0	0	0	tree	5.18.1115.4433		中国	新疆	昌吉州	电信	
//2	-1	0	0	-1	3	0	0	

public class VVMnameMappingMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text k = new Text();
	private Text v = new Text();

	String uid = null;
	String type = null;

	String aid = null;
	String wid = null;

	String mname = null;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();

		String[] arr = valueStr.split("\t", -1);
		if (arr.length == 32) {// vv 31 fields
			uid = arr[6].trim();
			aid = arr[8].trim();
			wid = arr[9].trim();
			type = arr[11].trim();
			if (null != type && type.equals("2")) {// 成功vv
				if (null != aid && null != wid && null != uid
						&& aid.length() > 0 && wid.length() > 0
						&& uid.length() > 0) {
					k.set(aid + "\t" + wid);
					v.set(Const.ID_PREDIX + uid);
					context.write(k, v);
					context.getCounter("advindex-job1", "advindex_vv")
							.increment(1);
				}
			}
		} else if (arr.length == 1) { // mapping separator ,
			arr = valueStr.split(",", -1);
			if (arr.length == 6) { // mapping 6 fields
				mname = arr[5].trim();
				aid = arr[0].trim();
				wid = arr[2].trim();

				if (null != aid && null != wid && null != mname
						&& aid.trim().length() > 0 && wid.trim().length() > 0
						&& mname.trim().length() > 0) {
					k.set(aid + "\t" + wid);
					v.set(Const.ATTR_PREFIX + mname);
					context.write(k, v);
					context.getCounter("advindex-job1", "mapping_data")
							.increment(1);
				}
			}
		}
	}
}
