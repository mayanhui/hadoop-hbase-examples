package com.adintellig.hbase.ads.recom;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScanTagsMapper extends TableMapper<Text, Text> {
	private Text k = new Text();
	private Text v = new Text();

	protected void setup(
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
	}

	public void map(ImmutableBytesWritable row, Result columns,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException {
		String value = null;
		Set<String> tags = new HashSet<String>();
		try {
			for (KeyValue kv : columns.list()) {
				value = Bytes.toStringBinary(kv.getValue());
				tags.add(value);
			}

			for (String tag : tags) {
				this.k.set("UID_" + Bytes.toStringBinary(row.get()));

				this.v.set(tag);
				context.write(this.k, this.v);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage() + ", Row: "
					+ Bytes.toString(row.get()) + ", Value: " + value);
		}
	}
}