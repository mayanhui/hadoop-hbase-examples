package com.baofeng.advindex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//其他    Chaos;Head      sex male 78.16 78.16
//其他    Chaos;Head      sex female 21.84 21.84
//其他    Chaos;Head      age 10-19岁 80 37.38
//其他    Chaos;Head      age 20-29岁 101 47.2
//其他    Chaos;Head      age 30-39岁 22 10.28
//其他    Chaos;Head      age 40-49岁 7 3.27
//其他    Chaos;Head      age 50-59岁 4 1.87
//其他    Chaos;Head      job 教育/学生 100 58.82
//其他    Chaos;Head      job IT 27 15.88
//其他    Chaos;Head      job 政府/公共服务 8 4.71
//其他    Chaos;Head      job 建筑 6 3.53
//其他    Chaos;Head      job 电信/网络 6 3.53
//其他    Chaos;Head      job 金融/房产 6 3.53
//其他    Chaos;Head      job 传媒/娱乐 6 3.53
//其他    Chaos;Head      job 服务 5 2.94
//其他    Chaos;Head      job 汽车 3 1.76
//其他    Chaos;Head      job 农林/化工 3 1.76
//其他    Chaos;Head      edu 本科及以上 71 27.41
//其他    Chaos;Head      edu 大专 28 10.81
//其他    Chaos;Head      edu 高中 100 38.61
//其他    Chaos;Head      edu 初中 44 16.99
//其他    Chaos;Head      edu 小学 16 6.18



//20130101        00      3396    130101p2p必胜客CNY      10000   86.0    104137  侠客行
//20130101        00      3109    121217p2p脉动北京       10000   2.0     104137  侠客行
//20130101        00      3110    121217p2p脉动上海       10000   1.0     104137  侠客行
//20130101        00      3111    121217p2p脉动深圳       10000   1.0     104137  侠客行
//20130101        00      3395    130101p2pKFCdinner全国  10000   122.0   104137  侠客行
//20130101        00      3114    121217p2p脉动西安       10000   3.0     104137  侠客行
//20130101        00      3394    130101p2pKFCdinner定向  10000   30.0    104137  侠客行

public class AdidAttrMapper extends Mapper<LongWritable, Text, Text, Text> {

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
		String id = null;
		String mname = null;
		String attr = null;

		String arr[] = valueStr.split("\t", -1);
		if (arr.length == 8) {
			id = arr[2].trim();
			mname = arr[7].trim();

			k.set(mname);
			v.set(Const.ID_PREDIX + id);
			context.write(k, v);
			context.getCounter("advindex-job1", "advindex_hour_movie")
					.increment(1);
		} else if (arr.length == 3) {
			mname = arr[1].trim();
			attr = arr[2].trim();

			k.set(mname);
			v.set(Const.ATTR_PREFIX + attr);
			context.write(k, v);
			context.getCounter("advindex-job1", "crowd_attr").increment(1);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}
