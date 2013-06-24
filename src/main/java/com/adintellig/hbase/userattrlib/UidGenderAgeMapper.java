package com.adintellig.hbase.userattrlib;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class UidGenderAgeMapper extends TableMapper<Writable, Writable> {

//	private byte[] columnFamily;
	private byte[] columnQualifier;

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

		String rowkey = new String(row.get());
		String gender = null;
		String age = null;

		try {
			for (KeyValue kv : columns.list()) {
				value = Bytes.toStringBinary(kv.getValue());
//				columnFamily = kv.getFamily();
				columnQualifier = kv.getQualifier();

				if (Bytes.toString(columnQualifier).equals("age"))
					age = value;
				else if (Bytes.toString(columnQualifier).equals("gender"))
					gender = value;
			}

			k.set(rowkey);
			v.set(gender + "\t" + age);

			context.write(k, v);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage() + ", Row: "
					+ Bytes.toString(row.get()) + ", Value: " + value);
		}
	}

}
