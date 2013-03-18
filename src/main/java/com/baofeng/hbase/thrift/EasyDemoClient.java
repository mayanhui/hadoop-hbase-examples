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
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class EasyDemoClient {

	static protected int port = 9090;
	static protected String host = "114.112.82.61";
	CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

	ByteBuffer table = ByteBuffer
			.wrap(bytes("user_behavior_attribute_noregistered_index"));
	// ByteBuffer table = ByteBuffer.wrap(bytes("demo_table"));
//	ByteBuffer columns = ByteBuffer.wrap(bytes("attr:adid"));
	ByteBuffer columns = ByteBuffer.wrap(bytes("cf1:rk"));
//	ByteBuffer row = ByteBuffer
//			.wrap(bytes("{F2DE8721-B4B6-5B87-E4CB-90FA4F0CA0B8}"));
	ByteBuffer row = ByteBuffer
			.wrap(bytes("attr:movt_area_欧美_type_电影"));
	

	// ByteBuffer row = ByteBuffer.wrap(bytes("2"));

	EasyDemoClient() {
	}

	EasyDemoClient(String fileName, int spleepTime) {
	}

	public static void main(String[] args) throws IOError, TException,
			IllegalArgument, AlreadyExists, IOException {

		EasyDemoClient client = new EasyDemoClient();
		client.run();
		// client.delete();
		// client.put();
//		client.get();
	}

	public void delete() throws IOError, TException {
		TTransport transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport, true, true);
		Hbase.Client client = new Hbase.Client(protocol);
	
		transport.open();
		System.out.println("Delete...");
		client.deleteAllTs(table, row, columns, 1358503174911L, null);
		transport.close();
	}

	public void put() throws IOError, IllegalArgument, TException {
		TTransport transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport, true, true);
		Hbase.Client client = new Hbase.Client(protocol);
		transport.open();

		ArrayList<Mutation> mutations = new ArrayList<Mutation>();
		mutations.add(new Mutation(false, columns, ByteBuffer
				.wrap(bytes("23233")), false));
		client.mutateRowTs(table, row, mutations, 1358503174910L, null);

		transport.close();
	}

	public void get() throws IOError, TException {
		TTransport transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport, true, true);
		Hbase.Client client = new Hbase.Client(protocol);

		transport.open();

		// List list = client.getVer(table, row,
		// columns, 1000, null);
		List<ByteBuffer> cols = new ArrayList<ByteBuffer>();
		cols.add(columns);
		cols.add(ByteBuffer.wrap(bytes("msg:status")));
		cols.add(ByteBuffer.wrap(bytes("msg:title")));
		List<TRowResult> list = client
				.getRowWithColumns(table, row, cols, null);
		// list = clijent.getRow(table, row, null);
		List<TCell> cells = client.getVerTs(table, row,
				ByteBuffer.wrap(bytes("msg:title")), 3L, 1, null);
		System.out.println(cells);
		// printRow(list);
		transport.close();
	}

	private void run() throws IOError, TException, IllegalArgument,
			AlreadyExists, IOException {
		// init
		TTransport transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport, true, true);
		Hbase.Client client = new Hbase.Client(protocol);

		transport.open();

		List list = client.getVer(table,row,
				columns, 1000, null);
		System.out.println(list);
		System.out.println(list.size());
//		 printRow(list);
		transport.close();
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
			ByteBuffer.wrap(buf);
			System.out.println(decoder);
			decoder.decode(ByteBuffer.wrap(buf));
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
