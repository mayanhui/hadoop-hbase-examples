package com.baofeng.hbase.ads.recom;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.baofeng.util.Const;

public class ScanTagsMapper extends TableMapper<Text, Text> {
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
		Set<String> tags = new HashSet<String>();
		try {
			for (KeyValue kv : columns.list()) {
				value = Bytes.toStringBinary(kv.getValue());
				tags.add(value);
			}

			for (String tag : tags) {
				k.set(Const.HBASE_ADS_RECOM_UID
						+ Bytes.toStringBinary(row.get()));
				v.set(tag);
				context.write(k, v);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage() + ", Row: "
					+ Bytes.toString(row.get()) + ", Value: " + value);
		}
	}

}
