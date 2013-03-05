package com.baofeng.advindex.user;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import com.baofeng.util.FileUtil;

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

public class UidAttrMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text k = new Text();
	private Text v = new Text();

	private Map<String, List<String>> mapping = new HashMap<String, List<String>>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		try {
			URI[] uris = DistributedCache.getCacheFiles(context
					.getConfiguration());
			for (URI uri : uris) {
				parseCacheFile(new Path(uri.getPath()),
						context.getConfiguration());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void parseCacheFile(Path cachFile, Configuration conf)
			throws IOException {
		String local = "/tmp/advindex_crowd_attrs";
		FileSystem fs = FileSystem.get(conf);
		// delete a directory
		FileUtil.deleteFile(new File(local));
		Path localPath = new Path(local, cachFile.getName());
		fs.copyToLocalFile(cachFile, localPath);
		
		BufferedReader fis = null;
		try {
			fis = new BufferedReader(new FileReader(localPath.toString()));
			String line = null;
			while ((line = fis.readLine()) != null) {
				String[] arr = line.split("\t", -1);
				if (arr.length == 3) {
					String mname = arr[1].trim();
					String attr = arr[2].trim();

					List<String> attrList = mapping.get(mname);
					if (null != attrList && attrList.size() >= 0) {
						attrList.add(attr);
						mapping.put(mname, attrList);
					} else {
						attrList = new ArrayList<String>();
						attrList.add(attr);
						mapping.put(mname, attrList);
					}
				}
			}
		} catch (IOException ioe) {
			System.err
					.println("Caught exception while parsing the cached file '"
							+ StringUtils.stringifyException(ioe));
		} finally {
			if (null != fis)
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		System.out.println("map size: " + mapping.size());

	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String id = null;
		String mname = null;

		String arr[] = valueStr.split("\t", -1);
		if (arr.length == 8) {
			id = arr[2].trim();
			mname = arr[7].trim();

			List<String> attrList = mapping.get(mname);

			if (null != attrList && attrList.size() > 0) {
				for (String attr : attrList) {
					k.set(id);
					v.set(attr);
					context.write(k, v);
					context.getCounter("advindex-job1", "advindex_hour_movie")
							.increment(1);
				}
			}
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}
