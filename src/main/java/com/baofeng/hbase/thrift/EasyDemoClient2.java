package com.baofeng.hbase.thrift;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
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
	String columns = "bhvr:search";

	public String fileName = "";
	public int sleepTime = 0;

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

		if (args.length < 1) {
			System.out.println("Parameters needed! >=1");
			return;
		}

		port = 9090;
		host = "114.112.82.61";
		String fileName = args[0];
		int sleepTime = 0;
		if(args.length == 2){
			sleepTime = Integer.parseInt(args[1]);
		}

		EasyDemoClient2 client = new EasyDemoClient2(fileName, sleepTime);
		client.run();
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
			
			if(count % 1000 == 0){
				try {
					System.out.println("sleep " + sleepTime +"ms!");
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

		System.out.println("Count: " + count);
		
		transport.close();
		br.close();
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
