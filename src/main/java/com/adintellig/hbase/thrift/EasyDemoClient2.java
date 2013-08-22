package com.adintellig.hbase.thrift;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.SortedMap;

import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class EasyDemoClient2 {

	static protected int port;
	static protected String host;
	CharsetDecoder decoder = null;

	String table = "user_behavior_attribute_noregistered";
	String columns = "bhvr:vvmid";

	public String fileName = "";
	public int sleepTime = 0;

	EasyDemoClient2() {
	}

	EasyDemoClient2(String fileName) {
		this(fileName, 0);
	}

	EasyDemoClient2(String fileName, int spleepTime) {
		decoder = Charset.forName("UTF-8").newDecoder();
		this.fileName = fileName;
		this.sleepTime = spleepTime;
	}

	public static void main(String[] args) throws IOError, TException,
			IllegalArgument, AlreadyExists, IOException {

		// if (args.length < 1) {
		// System.out.println("Parameters needed! >=1");
		// return;
		// }

		port = 9090;
		host = "119.188.128.182";
		// String fileName = args[0];
		// int sleepTime = 0;
		// if (args.length == 2) {
		// sleepTime = Integer.parseInt(args[1]);
		// }

		EasyDemoClient2 client = new EasyDemoClient2();
		client.run3();
	}

	private void run3() throws IOError, TException, IllegalArgument,
			AlreadyExists, IOException {
		// init
		TTransport transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport, true, true);
		Hbase.Client client = new Hbase.Client(protocol);

		transport.open();

		// List<ByteBuffer> columnList = new ArrayList<ByteBuffer>();
		// columnList.add(ByteBuffer.wrap(bytes(columns)));
		// List<TRowResult> rows = client.getRowWithColumns(
		// ByteBuffer.wrap(bytes(table)), ByteBuffer.wrap(bytes(row)),
		// columnList, null);

		List<TRowResult> results = client.getRow(ByteBuffer
				.wrap(bytes("user_test2")), ByteBuffer
				.wrap(bytes("{02FAF4E5-A3AE-8E64-91B5-9FE0B03B98C2}")), null);
		
		for(TRowResult r : results){
			System.out.println(Bytes.toString(r.getRow()));
			
			Map<ByteBuffer,TCell> cols = r.getColumns();
			StringBuilder rowStr = new StringBuilder();
			for (SortedMap.Entry<ByteBuffer, TCell> entry : cols.entrySet()) {
				rowStr.append(Bytes.toString(entry.getKey().array()));
				rowStr.append(" => ");
				rowStr.append(Bytes.toString(entry.getValue().value.array()));
				rowStr.append("\n ");
				
			}
			
			System.out.println(rowStr.toString());
			System.out.println(r.toString());
		}
		// List<TCell> cells = client.get(ByteBuffer.wrap(bytes("user_test2")),
		// ByteBuffer.wrap(bytes("{02FAF4E5-A3AE-8E64-91B5-9FE0B03B98C2}")),
		// null, null);

		transport.close();
	}

	private void run() throws IOError, TException, IllegalArgument,
			AlreadyExists, IOException {
		// init
		TTransport transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport, true, true);
		Hbase.Client client = new Hbase.Client(protocol);

		transport.open();

		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(fileName)));
		System.out.println("Read..." + fileName);
		String line = null;
		int count = 0;
		while (null != (line = br.readLine())) {
			long st = System.currentTimeMillis();
			line = line.trim();
			String[] arr = line.split("\t", -1);
			if (arr.length == 2) {
				String row = arr[0].trim();
				List<ByteBuffer> columnList = new ArrayList<ByteBuffer>();
				columnList.add(ByteBuffer.wrap(bytes(columns)));
				List<TRowResult> rows = client.getRowWithColumns(
						ByteBuffer.wrap(bytes(table)),
						ByteBuffer.wrap(bytes(row)), columnList, null);
				printRow(rows);
			}
			long en = System.currentTimeMillis();
			System.out.println("Time cost: " + (en - st));
			++count;

			// if (count % 1000 == 0) {
			// try {
			// System.out.println("sleep " + sleepTime + "ms!");
			// Thread.sleep(sleepTime);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
			// }

		}

		System.out.println("Count: " + count);

		transport.close();
		br.close();
	}

	private void run2() {

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					fileName)));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		System.out.println("Read..." + fileName);
		String line = null;
		int count = 0;
		long start = System.currentTimeMillis();
		try {
			while (null != (line = br.readLine())) {
				long st = System.currentTimeMillis();
				TTransport transport = new TSocket(host, port);
				try {
					TProtocol protocol = new TBinaryProtocol(transport, true,
							true);
					Hbase.Client client = new Hbase.Client(protocol);

					transport.open();
					line = line.trim();
					String[] arr = line.split("\t", -1);
					if (arr.length == 2) {
						String row = arr[0].trim();
						List<ByteBuffer> columnList = new ArrayList<ByteBuffer>();
						columnList.add(ByteBuffer.wrap(bytes(columns)));
						List<TRowResult> rows = client.getRowWithColumns(
								ByteBuffer.wrap(bytes(table)),
								ByteBuffer.wrap(bytes(row)), columnList, null);

						printRow(rows);
					}
					++count;
				} catch (Exception e) {
					System.out.println(new Date() + e.getMessage());
					++count;
					long en = System.currentTimeMillis();
					System.out.println("[2]Time cost: " + (en - st));
					System.out.println("Count: " + count);
					continue;
				}
				// if (count % 1000 == 0) {
				// try {
				// System.out.println("sleep " + sleepTime + "ms!");
				// Thread.sleep(sleepTime);
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// }

				transport.close();

				long en = System.currentTimeMillis();
				System.out.println("[2]Time cost: " + (en - st) + "ms");
				System.out.println("Count: " + count);
			}

			long end = System.currentTimeMillis();
			System.out.println("AVG: " + ((end - start) / count) + "ms");
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private final void printRow(TRowResult rowResult) {
		// copy values into a TreeMap to get them in sorted order
		TreeMap<String, TCell> sorted = new TreeMap<String, TCell>();
		for (Map.Entry<ByteBuffer, TCell> column : rowResult.columns.entrySet()) {
			sorted.put(utf8(column.getKey().array()), column.getValue());
		}

		StringBuilder rowStr = new StringBuilder();
		for (SortedMap.Entry<String, TCell> entry : sorted.entrySet()) {
			rowStr.append(entry.getKey());
			rowStr.append(" => ");
			rowStr.append(utf8(entry.getValue().value.array()));
			rowStr.append("; ");
		}
		System.out.println("row: " + utf8(rowResult.row.array()) + ", cols: "
				+ rowStr);
	}

	private void printRow(List<TRowResult> rows) {
		for (TRowResult rowResult : rows) {
			printRow(rowResult);
		}
	}

	// Helper to translate byte[]'s to UTF8 strings
	private String utf8(byte[] buf) {
		try {
			return decoder.decode(ByteBuffer.wrap(buf)).toString();
		} catch (CharacterCodingException e) {
			return "[INVALID UTF-8]";
		}
	}

	// Helper to translate strings to UTF8 bytes
	private byte[] bytes(String s) {
		try {
			return s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}
}
