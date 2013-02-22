package com.baofeng.hbase.ads.recom;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class AdidListLoadMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
	private byte[] family = null;
	private byte[] qualifier = null;
	private String rowkey = null;
	private String colValue = null;

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

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();

		String arr[] = valueStr.split("\t", -1);
		if (arr.length == 2) {
			rowkey = arr[0];
			colValue = arr[1];
			if (rowkey == null || rowkey.trim().length() == 0
					|| null == colValue || colValue.trim().length() == 0)
				return;

			Put put = new Put(Bytes.toBytes(rowkey));
			put.add(family, qualifier, Bytes.toBytes(colValue));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)),
					put);
			context.getCounter("hbase-import", "line").increment(1);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}
