package com.adintellig.hbase.ads.recom;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UidAdidMapper extends Mapper<LongWritable, Text, Text, Text>
{
  private Text k = new Text();
  private Text v = new Text();

  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    String valueStr = value.toString();
    String[] arr = valueStr.split("\t", -1);
    if (arr.length == 2) {
      this.k.set(arr[1].trim());
      this.v.set(arr[0].trim());
      context.write(this.k, this.v);
    }
  }
}