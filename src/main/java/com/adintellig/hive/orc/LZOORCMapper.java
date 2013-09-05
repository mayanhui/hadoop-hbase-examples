package com.adintellig.hive.orc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class LZOORCMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, NullWritable, Writable> {

	@Override
	public void configure(JobConf job) {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<NullWritable, Writable> output, Reporter reporter)
			throws IOException {
		String valueStr = value.toString();
		OrcSerde serde = new OrcSerde();

		StructObjectInspector inspector;
		inspector = (StructObjectInspector) ObjectInspectorFactory
				.getReflectionObjectInspector(Row.class,
						ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

		Row row = new Row();
		String[] arr = valueStr.split("\t", -1);
		if (arr.length >= 33) {
			row.stat_date = arr[0];
			row.stat_hour = arr[1];
			row.ip = arr[2];
			row.logdate = arr[3];
			row.method = arr[3];
			row.url = arr[5];
			row.uid = arr[6];
			row.pid = arr[7];
			try {
				row.aid = (arr[8].equals("") || null == arr[8]) ? 0 : Integer
						.parseInt(arr[8]);
				row.wid = (arr[9].equals("") || null == arr[9]) ? 0 : Integer
						.parseInt(arr[9]);
				row.vid = (arr[10].equals("") || null == arr[10]) ? 0 : Integer
						.parseInt(arr[10]);
				row.type = (arr[11].equals("") || null == arr[11]) ? 0
						: Integer.parseInt(arr[11]);
				row.stat = (arr[12].equals("") || null == arr[12]) ? 0
						: Integer.parseInt(arr[12]);
				row.mtime = (arr[13].equals("") || null == arr[13]) ? 0.0f
						: Float.parseFloat(arr[13]);
				row.ptime = (arr[14].equals("") || null == arr[14]) ? 0.0f
						: Float.parseFloat(arr[14]);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			row.channel = arr[15];
			row.boxver = arr[16];
			try {
				row.bftime = (arr[17].equals("") || null == arr[17]) ? 0
						: Integer.parseInt(arr[17]);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			row.country = arr[18];
			row.province = arr[19];
			row.city = arr[20];
			row.isp = arr[21];
			try {
				row.ditchid = (arr[22].equals("") || null == arr[22]) ? 0
						: Integer.parseInt(arr[22]);
				row.drm = (arr[23].equals("") || null == arr[23]) ? 0 : Integer
						.parseInt(arr[23]);
				row.charge = (arr[24].equals("") || null == arr[24]) ? 0
						: Integer.parseInt(arr[24]);
				row.ad = (arr[25].equals("") || null == arr[25]) ? 0 : Integer
						.parseInt(arr[25]);
				row.adclick = (arr[26].equals("") || null == arr[26]) ? 0
						: Integer.parseInt(arr[26]);
				row.groupid = (arr[27].equals("") || null == arr[27]) ? 0
						: Integer.parseInt(arr[27]);
				row.client = (arr[28].equals("") || null == arr[28]) ? 0
						: Integer.parseInt(arr[28]);
				row.usertype = (arr[29].equals("") || null == arr[29]) ? 0
						: Integer.parseInt(arr[29]);
				row.ptolemy = (arr[30].equals("") || null == arr[30]) ? 0
						: Integer.parseInt(arr[30]);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			row.fixedid = arr[31];
			row.userid = arr[32];

		}
		Writable v = serde.serialize(row, inspector);

		output.collect(NullWritable.get(), v);
	}

	static class Row implements Writable {
		String stat_date;
		String stat_hour;
		String ip;
		String logdate;
		String method;
		String url;
		String uid;
		String pid;
		int aid;
		int wid;
		int vid;
		int type;
		int stat;
		float mtime;
		float ptime;
		String channel;
		String boxver;
		int bftime;
		String country;
		String province;
		String city;
		String isp;
		int ditchid;
		int drm;
		int charge;
		int ad;
		int adclick;
		int groupid;
		int client;
		int usertype;
		int ptolemy;
		String fixedid;
		String userid;

		public Row() {
		}

		public Row(String stat_date, String stat_hour, String ip,
				String logdate, String method, String url, String uid,
				String pid, int aid, int wid, int vid, int type, int stat,
				float mtime, float ptime, String channel, String boxver,
				int bftime, String country, String province, String city,
				String isp, int ditchid, int drm, int charge, int ad,
				int adclick, int groupid, int client, int usertype,
				int ptolemy, String fixedid, String userid) {
			super();
			this.stat_date = stat_date;
			this.stat_hour = stat_hour;
			this.ip = ip;
			this.logdate = logdate;
			this.method = method;
			this.url = url;
			this.uid = uid;
			this.pid = pid;
			this.aid = aid;
			this.wid = wid;
			this.vid = vid;
			this.type = type;
			this.stat = stat;
			this.mtime = mtime;
			this.ptime = ptime;
			this.channel = channel;
			this.boxver = boxver;
			this.bftime = bftime;
			this.country = country;
			this.province = province;
			this.city = city;
			this.isp = isp;
			this.ditchid = ditchid;
			this.drm = drm;
			this.charge = charge;
			this.ad = ad;
			this.adclick = adclick;
			this.groupid = groupid;
			this.client = client;
			this.usertype = usertype;
			this.ptolemy = ptolemy;
			this.fixedid = fixedid;
			this.userid = userid;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			throw new UnsupportedOperationException("no write");
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			throw new UnsupportedOperationException("no read");
		}
	}

}
