package com.baofeng.hbase.vv;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class VVImportMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

	private byte[] family = null;
	private byte[] qualifier = null;
	private String uid = null;
	private String mid = null;
	private long ts = System.currentTimeMillis();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		String column = context.getConfiguration().get("conf.column");
		byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
		family = colkey[0];
		if (colkey.length > 1) {
			qualifier = colkey[1];
		}
	}

	// "{2C39C0F8-55C7-38A0-1DEF-F7448D1BB80D}",59177,1341453584000
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			String lineString = value.toString();
			if (lineString.indexOf("uid") >= 0
					&& lineString.indexOf("mid") >= 0
					&& lineString.indexOf("ts") >= 0)
				return;
			String[] arr = lineString.split(",", -1);
			if (arr.length == 3) {
				uid = arr[0];
				if (null != uid && uid.length() > 0 && uid.startsWith("\"")
						&& uid.endsWith("\"")) {
					uid = uid.substring(1, uid.length() - 1);
				}
				mid = arr[1];
				String tsStr = arr[2];
				if (null != tsStr && tsStr.length() > 0) {
					ts = Long.parseLong(tsStr);
				}
			}

			byte[] rowkey = uid.getBytes();
			Put put = new Put(rowkey, ts);
			put.add(family, qualifier, Bytes.toBytes(mid));
			context.write(new ImmutableBytesWritable(rowkey), put);
			context.getCounter("hbase-import", "vv-line").increment(1);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}
