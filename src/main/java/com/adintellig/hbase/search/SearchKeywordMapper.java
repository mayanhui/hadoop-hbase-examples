package com.adintellig.hbase.search;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.adintellig.util.Const;




public class SearchKeywordMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
	private byte[] family = null;
	private byte[] qualifier = null;
	private String uid = null;
	private String keyword = null;
	List<String> invalid = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		// load invalid characters
		invalid = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				SearchKeywordMapper.class
						.getResourceAsStream(Const.INVALID_CHARACTOR_PATH)));
		String line = null;
		while (null != (line = br.readLine())) {
			line = line.trim();
			invalid.add(line);
		}
		br.close();

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
			uid = arr[0];
			keyword = arr[1];
			
			// clean data
			for (String ic : invalid) {
				if (keyword.indexOf(ic) >= 0){
					System.out.println(ic + "||" + keyword);
					return;
				}
			}

			byte[] rowkey = uid.getBytes();
			Put put = new Put(rowkey);
			put.add(family, qualifier, Bytes.toBytes(keyword));
			context.write(new ImmutableBytesWritable(rowkey), put);
			context.getCounter("hbase-import", "keyword-line").increment(1);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}
