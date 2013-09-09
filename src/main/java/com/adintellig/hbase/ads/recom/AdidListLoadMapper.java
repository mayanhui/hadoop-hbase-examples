package com.adintellig.hbase.ads.recom;

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

	protected void setup(
			Mapper<LongWritable, Text, ImmutableBytesWritable, Writable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		String column = context.getConfiguration().get("conf.column");
		byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
		this.family = colkey[0];
		if (colkey.length > 1)
			this.qualifier = colkey[1];
	}

	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Writable>.Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();

		String[] arr = valueStr.split("\t", -1);
		if (arr.length == 2) {
			this.rowkey = arr[0];
			this.colValue = arr[1];
			if ((this.rowkey == null) || (this.rowkey.trim().length() == 0)
					|| (null == this.colValue)
					|| (this.colValue.trim().length() == 0)) {
				return;
			}
			Put put = new Put(Bytes.toBytes(this.rowkey));
			put.add(this.family, this.qualifier, Bytes.toBytes(this.colValue));
			context.write(
					new ImmutableBytesWritable(Bytes.toBytes(this.rowkey)), put);

			context.getCounter("hbase-import", "line").increment(1L);
		}
	}

	protected void cleanup(
			Mapper<LongWritable, Text, ImmutableBytesWritable, Writable>.Context context)
			throws IOException, InterruptedException {
	}
}