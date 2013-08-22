package com.adintellig.hbase.userattrlib.cptable3;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Dumper extends TableMapper<Writable, Writable> {

	private byte[] columnFamily;
	private byte[] columnQualifier;
	private long timestamp;

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
//		long st = System.currentTimeMillis();
//		String value = null;

		try {
//			int count = 0;
//			long stepTime = 0L;
			for (KeyValue kv : columns.list()) {
//				count++;
				if (kv == null)
					continue;

				timestamp = kv.getTimestamp();

				// 2014-01-01 00:00:00  || 2012-01-01 00:00:00
//				if (timestamp >= 1388505600000L || timestamp <= 1325347200000L)
//					continue;

				if (!kv.isDelete()) {
					columnFamily = kv.getFamily();
					columnQualifier = kv.getQualifier();
					String value = Bytes.toStringBinary(kv.getValue());

					k.set(row.get());
					v.set(Bytes.toString(columnFamily) + FIELD_COMMON_SEPARATOR
							+ Bytes.toString(columnQualifier)
							+ FIELD_COMMON_SEPARATOR + value
							+ FIELD_COMMON_SEPARATOR + timestamp);

					context.write(k, v);
				}
			}

//			long en = System.currentTimeMillis();
//			System.out.println((en - st) + "ms" + ", " + count + ", "
//					+ stepTime + "ms");

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage() + ", Row: "
					+ Bytes.toString(row.get()));
		}
	}

}
