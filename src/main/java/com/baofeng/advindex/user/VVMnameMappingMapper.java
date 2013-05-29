package com.baofeng.advindex.user;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

//import com.baofeng.advindex.Const;
import com.baofeng.util.FileUtil;
import com.baofeng.util.LzoUtil;

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

	private Map<String, String> mapping = new HashMap<String, String>();

	@Override
	protected void setup(Context context) {
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
		String local = "/tmp/advindex_user";
		FileSystem fs = FileSystem.get(conf);
		// delete a directory
		boolean delete = FileUtil.deleteFile(new File(local));
		if (delete) {
			fs.copyToLocalFile(cachFile, new Path(local,
					"movieid-mapping.txt.lzo"));

			LzoUtil.unCompressLzo(local + File.separator
					+ "movieid-mapping.txt.lzo", local + File.separator
					+ "movieid-mapping.txt", conf);

			BufferedReader fis = null;
			try {
				fis = new BufferedReader(new FileReader(local + File.separator
						+ "movieid-mapping.txt"));
				String line = null;
				while ((line = fis.readLine()) != null) {
					String[] arr = line.split(",");
					if (arr.length == 6) { // mapping 6 fields
						String mname = arr[5].trim();
						String aid = arr[0].trim();
						String wid = arr[2].trim();

						mapping.put(aid + "\t" + wid, mname);
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
		}
		System.out.println("map size: " + mapping.size());

	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String[] arr = valueStr.split("\t", -1);
		if (arr.length >= 32) {// vv 31 fields + 1 tab
			uid = arr[6].trim();
			aid = arr[8].trim();
			wid = arr[9].trim();
			type = arr[11].trim();
			if (null != type && type.equals("2")) {// 成功vv
				if (null != aid && null != wid && null != uid
						&& aid.length() > 0 && wid.length() > 0
						&& uid.length() > 0) {
					String mname = mapping.get(aid + "\t" + wid);
					if (null != mname && mname.length() > 0) {
						k.set("0\t1\t" + uid);
						v.set("3\t4\t5\t6\t" + mname);
						context.write(k, v);
						context.getCounter("advindex-job1", "advindex_vv")
								.increment(1);
					}
				}
			}
		}
	}
}
