package com.baofeng.hbase.ads.user;

import java.io.IOException;
//import java.util.HashSet;
//import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class ScanUserAttrMapper extends TableMapper<Text, Text> {
	private Text k = new Text();
	private Text v = new Text();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	}

	@Override
	public void map(ImmutableBytesWritable row, Result columns, Context context)
			throws IOException {
		String value = null;
		try {
			for (KeyValue kv : columns.list()) {
				value = Bytes.toStringBinary(kv.getValue());
				break;
			}
			if (null != value && value.trim().length() > 0) {
				k.set(Bytes.toStringBinary(row.get()));
				v.set(value);
				context.write(k, v);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage() + ", Row: "
					+ Bytes.toString(row.get()) + ", Value: " + value);
		}
	}

}
