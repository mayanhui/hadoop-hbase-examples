package com.adintellig.hbase.vv.dumpimport;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DumpVVMidMapper extends TableMapper<Writable, Writable> {

	private Text k = new Text();
	private Text v = new Text();

	public static final String FIELD_COMMON_SEPARATOR = "\u0001";

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	}

	@Override
	public void map(ImmutableBytesWritable row, Result columns, Context context)
			throws IOException {
		String value = null;

		String rowkey = new String(row.get());
		byte[] columnFamily = null;
		byte[] columnQualifier = null;
		long ts = 0L;

		try {
			for (KeyValue kv : columns.list()) {
				value = Bytes.toStringBinary(kv.getValue());
				columnFamily = kv.getFamily();
				columnQualifier = kv.getQualifier();
				ts = kv.getTimestamp();

				k.set(rowkey);
				v.set(Bytes.toString(columnFamily) + FIELD_COMMON_SEPARATOR
						+ Bytes.toString(columnQualifier)
						+ FIELD_COMMON_SEPARATOR + value
						+ FIELD_COMMON_SEPARATOR + ts);

				context.write(k, v);
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage() + ", Row: "
					+ Bytes.toString(row.get()) + ", Value: " + value);
		}
	}

}
