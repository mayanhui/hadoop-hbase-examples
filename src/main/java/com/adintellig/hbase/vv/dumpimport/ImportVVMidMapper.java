package com.adintellig.hbase.vv.dumpimport;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class ImportVVMidMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

	public static final String FIELD_COMMON_SEPARATOR = "\u0001";

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	// {F0106543-AA91-D47B-2230-0FC27F53D73F} bhvrvvmid2308091375035271000
	// {F0106543-AA91-D47B-2230-0FC27F53D73F} bhvrvvmid2338311375033231000
	// {F0106543-AA91-D47B-2230-0FC27F53D73F} bhvrvvmid1660771374940502000
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			String lineString = value.toString();
			String[] arr = lineString.split("\t", -1);
			if (arr.length == 2) {
				String uid = arr[0];
				String[] karr = arr[1].split(FIELD_COMMON_SEPARATOR, -1);
				if (karr.length == 4) {
					byte[] columnFamily = karr[0].getBytes();
					byte[] columnQualifier = karr[1].getBytes();
					byte[] columnValue = karr[2].getBytes();
					long columnTs = Long.parseLong(karr[3]);

					Put put = new Put(uid.getBytes(), columnTs);
					put.add(columnFamily, columnQualifier, columnValue);
					context.write(new ImmutableBytesWritable(uid.getBytes()),
							put);
					context.getCounter("hbase-import", "vvmid").increment(1);
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}
