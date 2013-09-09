package com.adintellig.hbase.ads.user;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScanUserAttrMapper extends TableMapper<Text, Text>
{
  private Text k = new Text();
  private Text v = new Text();

  protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
  }

  public void map(ImmutableBytesWritable row, Result columns, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
    throws IOException
  {
    String value = null;
    try {
      Iterator<?> i$ = columns.list().iterator(); if (i$.hasNext()) { KeyValue kv = (KeyValue)i$.next();
        value = Bytes.toStringBinary(kv.getValue());
      }

      if ((null != value) && (value.trim().length() > 0)) {
        this.k.set(Bytes.toStringBinary(row.get()));
        this.v.set(value);
        context.write(this.k, this.v);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error: " + e.getMessage() + ", Row: " + Bytes.toString(row.get()) + ", Value: " + value);
    }
  }
}