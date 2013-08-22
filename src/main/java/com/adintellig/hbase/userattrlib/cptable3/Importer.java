package com.adintellig.hbase.userattrlib.cptable3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.adintellig.hbase.userattrlib.cptable.VV;
import com.adintellig.hbase.userattrlib.cptable.Video;

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
		long st = System.currentTimeMillis();
		String value = null;

		try {
			Put put = null;
//			Delete delete = null;
			int count = 0;
			long stepTime = 0L;
			for (KeyValue kv : columns.list()) {
				count ++;
				if (kv == null)
					continue;

				timestamp = kv.getTimestamp();

				// 2014-01-01 00:00:00
				if (timestamp >= 1388505600000L)
					continue;

				// if (kv.isDelete()) {
				// if (delete == null) {
				// delete = new Delete(row.get());
				// }
				// delete.addDeleteMarker(kv);
				// } else 
				
				if(!kv.isDelete()){
					if (put == null) {
						put = new Put(row.get());
					}

					columnFamily = kv.getFamily();
					columnQualifier = kv.getQualifier();
					value = Bytes.toStringBinary(kv.getValue());
					
					long stepSt = System.currentTimeMillis();
					if (Bytes.toString(columnFamily).equals("bhvr")
							&& Bytes.toString(columnQualifier).equals("vvmid")) {
						// get aid & type
						String aid = null;
						String mid = null;
						String wid = null;
//						String mtime = null;
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
//							mtime = arr[3].trim();
							type = arr[4].trim();
							name = arr[5].trim();
						}
						if (null == aid || null == type)
							continue;

						String columnQualifierString = "M_" + value.trim()
								+ "_" + aid + "_" + type;

						VV vv = new VV();
						vv.setTime(timestamp);
						vv.setAid(aid);
						Video video = vv.getVideo();
						video.setAid(aid);
						video.setMovieid(mid);
						video.setWid(wid);
						video.setVtitle(name);

						String putValue = mapper.writeValueAsString(vv);

						put.add(columnFamily, columnQualifierString.getBytes(),
								timestamp, putValue.getBytes());
						long stepEn = System.currentTimeMillis();
						stepTime += stepEn - stepSt;
					} else
						put.add(kv);
				}

				if (null != put) {
					context.write(row, put);
				}
//				if (delete != null) {
//					context.write(row, delete);
//				}
			}

			long en = System.currentTimeMillis();
			System.out.println((en - st) + "ms" + ", " + count + ", " + stepTime + "ms");

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage() + ", Row: "
					+ Bytes.toString(row.get()) + ", Value: " + value);
		}
	}

}
