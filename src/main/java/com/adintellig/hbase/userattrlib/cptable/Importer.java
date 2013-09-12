package com.adintellig.hbase.userattrlib.cptable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.adintellig.util.UnicodeTransverter;

public class Importer extends TableMapper<Writable, Writable> {

	private byte[] columnFamily;
	private byte[] columnQualifier;
	private long timestamp;

	private Map<String, String> mapping;
	ObjectMapper mapper = new ObjectMapper();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mapping = new HashMap<String, String>();

		try {
			Path[] paths = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());

			for (Path path : paths) {
				parseCacheFile(path, context.getConfiguration());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// aid,movieid,wid,mtime,type,name
	private void parseCacheFile(Path cachFile, Configuration conf)
			throws IOException {
		BufferedReader fis = null;
		try {
			fis = new BufferedReader(new FileReader(cachFile.toString()));
			String line = null;
			while ((line = fis.readLine()) != null) {
				String[] arr = line.split(",", -1);
				if (arr.length >= 6) {
					String mid = arr[1].trim();
					mapping.put(mid, line);
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
	public void map(ImmutableBytesWritable row, Result columns, Context context)
			throws IOException {
		// long st = System.currentTimeMillis();
		String value = null;
		Put put = null;
		try {
			// Delete delete = null;
			// int count = 0;
			// long stepTime = 0L;
			for (KeyValue kv : columns.list()) {
				// count++;
				if (kv == null)
					continue;

				timestamp = kv.getTimestamp();

				// 2014-01-01 00:00:00 || 2012-01-01 00:00:00
				if (timestamp >= 1388505600000L || timestamp <= 1325347200000L)
					continue;

				// if (kv.isDelete()) {
				// if (delete == null) {
				// delete = new Delete(row.get());
				// }
				// delete.addDeleteMarker(kv);
				// } else

				if (!kv.isDelete()) {
					columnFamily = kv.getFamily();
					columnQualifier = kv.getQualifier();

					// long stepSt = System.currentTimeMillis();
					if (Bytes.toString(columnFamily).equals("bhvr")
							&& Bytes.toString(columnQualifier).equals("vvmid")) {
						value = Bytes.toStringBinary(kv.getValue());

						// get aid & type
						String aid = null;
						String mid = null;
						String wid = null;
						// String mtime = null;
						String name = null;
						String type = null;
						String line = mapping.get(value);
						if (null == line)
							continue;
						String[] arr = line.split(",", -1);
						if (arr.length >= 6) {
							aid = arr[0].trim();
							mid = arr[1].trim();
							wid = arr[2].trim();
							// mtime = arr[3].trim();
							type = arr[4].trim();
							name = arr[5].trim();
						}
						if (null == aid || null == type)
							continue;

						int typeID = transTypeId(type.trim());
						if (typeID == 0)
							continue;

						String columnQualifierString = "M_" + value.trim()
								+ "_" + aid + "_" + typeID;

						VV vv = new VV();
						vv.setTime(timestamp/1000);
						vv.setAid(aid);
						vv.setUid(Bytes.toString(row.get()));
						Video video = vv.getVideo();
						video.setAid(aid);
						video.setMovieid(mid);
						video.setWid(wid);
						video.setVtitle(name);

						String putValue = mapper.writeValueAsString(vv);
						System.out.println(putValue);
//						putValue = UnicodeTransverter.utf8ToUnicode(putValue);
						String temp = UnicodeTransverter.utf8ToUnicode(putValue);
						System.out.println(temp);
						
						if (put == null) {
							put = new Put(row.get());
						}
						put.add(columnFamily, columnQualifierString.getBytes(),
								timestamp, putValue.getBytes());
						// long stepEn = System.currentTimeMillis();
						// stepTime += stepEn - stepSt;
					}
					// else
					// put.add(kv);
				}
				// if (delete != null) {
				// context.write(row, delete);
				// }
			}

			if (put != null) {
				context.write(row, put);
			}

			// long en = System.currentTimeMillis();
			// System.out.println((en - st) + "ms" + ", " + count);

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage() + ", Row: "
					+ Bytes.toString(row.get()) + ", Value: " + value + "CF: "
					+ Bytes.toString(columnFamily) + "Column: "
					+ Bytes.toString(columnQualifier) + put.toJSON());
		}
	}

	private static int transTypeId(String cnType) {
		if (cnType == null)
			return 0;
		if (cnType.equals("电影")) {
			return 1;
		} else if (cnType.equals("电视")) {
			return 2;
		} else if (cnType.equals("动漫")) {
			return 3;
		} else if (cnType.equals("综艺")) {
			return 4;
		} else if (cnType.equals("其他")) {
			return 59;
		} else if (cnType.equals("微视频")) {
			return 60;
		} else if (cnType.equals("纪录片")) {
			return 63;
		} else if (cnType.equals("教育")) {
			return 66;
		} else if (cnType.equals("公开课")) {
			return 68;
		} else if (cnType.equals("微电影")) {
			return 70;
		}
		return 0;

	}

}
