package com.adintellig.hbase.thrift2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TIllegalArgument;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TTimeRange;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class DemoClient {
	public static void main(String[] args) throws TIOError, TException,
			TIllegalArgument {
		System.out.println("Thrift2 Demo");

		String host = "222.173.25.115";
		host = "222.173.25.101";
//		host = "114.112.82.24";
		int port = 9090;
		int timeout = 10000;
		boolean framed = false;

		TTransport transport = new TSocket(host, port, timeout);
		if (framed) {
			transport = new TFramedTransport(transport);
		}
		TProtocol protocol = new TBinaryProtocol(transport);
		// This is our thrift client.
		THBaseService.Iface client = new THBaseService.Client(protocol);

		// open the transport
		transport.open();

		String tableName = "user_behavior_attribute_noregistered";
		String rowkey = "{663FCA96-B865-8AB8-A416-9BBFB5F983CB}";
		String columnFamily = "bhvr";

		ByteBuffer table = ByteBuffer.wrap(tableName.getBytes());

		// (1)PUT

		// TPut put = new TPut();
		// put.setRow("row1".getBytes());
		//
		// TColumnValue columnValue = new TColumnValue();
		// columnValue.setFamily("family1".getBytes());
		// columnValue.setQualifier("qualifier1".getBytes());
		// columnValue.setValue("value1".getBytes());
		// List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
		// columnValues.add(columnValue);
		// put.setColumnValues(columnValues);
		//
		// client.put(table, put);

		// (2) GET
		// TGet get = new TGet();
		// get.setRow(rowkey.getBytes());
		//
		// TResult result = client.get(table, get);
		//
		// System.out.println("row = " + new String(result.getRow()));
		// for (TColumnValue resultColumnValue : result.getColumnValues()) {
		// System.out.println("family = "
		// + new String(resultColumnValue.getFamily()));
		// System.out.println("qualifier = "
		// + new String(resultColumnValue.getQualifier()));
		// System.out.println("value = "
		// + new String(resultColumnValue.getValue()));
		// System.out.println("timestamp = " +
		// resultColumnValue.getTimestamp());
		// }

		// (3) SCAN
		TScan scan = new TScan();
		// set rowkey
		scan.setStartRow(rowkey.getBytes());
		scan.setStopRow(rowkey.getBytes());
		// set timerange
		TTimeRange timeRange = new TTimeRange(1376450385000L, 13764503850000L);
		scan.setTimeRange(timeRange);
		// set column
		List<TColumn> columns = new ArrayList<TColumn>();
		TColumn column = new TColumn(ByteBuffer.wrap(columnFamily.getBytes()));
		columns.add(column);
		scan.setColumns(columns);

		// filter
		/* a. 找到以"FAV_"开头的所有列 */
		String filterString = "QualifierFilter(=,'regexstring:^FAV_.*')";
		
		/* b. 找到字段"FAV_42666" */
		// String filterString = "QualifierFilter(=,'binary:FAV_42666')";
		scan.setFilterString(filterString.getBytes());

		int scannerId = client.openScanner(table, scan);

		List<TResult> tResults = client.getScannerRows(scannerId,
				1);

		for (TResult r : tResults) {
			System.out.println("row = " + new String(r.getRow()));
			for (TColumnValue resultColumnValue : r.getColumnValues()) {
				System.out.println("family = "
						+ new String(resultColumnValue.getFamily()));
				System.out.println("qualifier = "
						+ new String(resultColumnValue.getQualifier()));
				System.out.println("value = "
						+ new String(resultColumnValue.getValue()));
				System.out.println("timestamp = "
						+ resultColumnValue.getTimestamp());
			}
		}

		transport.close();
	}
}