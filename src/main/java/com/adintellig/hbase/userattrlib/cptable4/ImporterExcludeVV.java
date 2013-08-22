package com.adintellig.hbase.userattrlib.cptable4;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class ImporterExcludeVV extends TableMapper<Writable, Writable> {

	private byte[] columnFamily;
	private byte[] columnQualifier;
	private long timestamp;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

	}

	@Override
	public void map(ImmutableBytesWritable row, Result columns, Context context)
			throws IOException {
		long st = System.currentTimeMillis();
		String value = null;

		try {
			Put put = null;
			int count = 0;
			for (KeyValue kv : columns.raw()) {
				count++;
				if (kv == null)
					continue;

				timestamp = kv.getTimestamp();

				// 2014-01-01 00:00:00 || 2012-01-01 00:00:00
				if (timestamp >= 1388505600000L || timestamp <= 1325347200000L)
					continue;

				if (!kv.isDelete()) {
					columnFamily = kv.getFamily();
					columnQualifier = kv.getQualifier();

					if (!Bytes.toString(columnFamily).equals("bhvr")
							&& !Bytes.toString(columnQualifier).equals("vvmid")) {
						put = new Put(row.get());
						put.add(kv);
						context.write(row, put);
					}
				}
			}

			long en = System.currentTimeMillis();
			System.out.println((en - st) + "ms" + ", " + count);

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage() + ", Row: "
					+ Bytes.toString(row.get()) + ", Value: " + value);
		}
	}

}
