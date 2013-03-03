package com.baofeng.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.hadoop.compression.lzo.LzopCodec;

public class LzoUtil {

	private static Configuration getDefaultConf() {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "local");
//		conf.set("fs.default.name", "file:///");
		conf.set("fs.default.name", "hdfs:///");
		conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");
		return conf;
	}

	public static void write2LzoFile(String destLzoFilePath,
			Configuration conf, byte[] datas) {
		LzopCodec lzo = null;
		OutputStream out = null;

		try {
			lzo = new LzopCodec();
			lzo.setConf(conf);
			out = lzo.createOutputStream(new FileOutputStream(destLzoFilePath));
			out.write(datas);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static List<String> readLzoFile(String lzoFilePath,
			Configuration conf) {
		LzopCodec lzo = null;
		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader reader = null;
		List<String> result = null;
		String line = null;

		try {
			lzo = new LzopCodec();
			lzo.setConf(conf);
			is = lzo.createInputStream(new FileInputStream(lzoFilePath));
			isr = new InputStreamReader(is);
			reader = new BufferedReader(isr);
			result = new ArrayList<String>();
			while ((line = reader.readLine()) != null) {
				result.add(line);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
				if (isr != null) {
					isr.close();
				}
				if (is != null) {
					is.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	public static void unCompressLzo(String input, String output,
			Configuration conf) {
		LzopCodec lzo = null;
		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader reader = null;
		String line = null;
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(output)));
			lzo = new LzopCodec();
			lzo.setConf(conf);
			is = lzo.createInputStream(new FileInputStream(input));
			isr = new InputStreamReader(is);
			reader = new BufferedReader(isr);
			while ((line = reader.readLine()) != null) {
				bw.write(line);
				bw.newLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != reader)
					reader.close();
				if (null != isr)
					isr.close();
				if (null != is)
					is.close();
				if (null != bw)
					bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage: hadoop jar *.jar className input output");
			return;
		}

		String input = args[0];
		String output = args[1];

		unCompressLzo(input, output, getDefaultConf());
	}
}