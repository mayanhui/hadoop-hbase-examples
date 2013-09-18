package com.adintellig.hbase.thrift2;

import java.lang.Character.UnicodeBlock;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TIllegalArgument;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TTimeRange;
import org.apache.hadoop.hbase.util.Bytes;
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
		// host = "222.173.25.101";
		// host = "114.112.82.24";
		
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
		tableName = "user_behavior_attribute";
		// tableName = "user_test";
		String rowkey = "{663FCA96-B865-8AB8-A416-9BBFB5F983CB}";
		 rowkey = "13122609814632914";
		// rowkey = "Log_EXPRESS";
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
		TTimeRange timeRange = new TTimeRange(137645038500L, 13764503850000L);
		scan.setTimeRange(timeRange);
		// set column
		List<TColumn> columns = new ArrayList<TColumn>();
		TColumn column = new TColumn(ByteBuffer.wrap(columnFamily.getBytes()));
		columns.add(column);
		scan.setColumns(columns);

		// filter
		/* a. 找到以"FAV_"开头的所有列 */
		String filterString = "QualifierFilter(=,'regexstring:^FAV_.*')";
//		filterString = "QualifierFilter(=,'regexstring:^M_.*')";
		/* b. 找到字段"FAV_42666" */
		// String filterString = "QualifierFilter(=,'binary:FAV_42666')";
		scan.setFilterString(filterString.getBytes());

		int scannerId = client.openScanner(table, scan);

		List<TResult> tResults = client.getScannerRows(scannerId, 1);

		for (TResult r : tResults) {
			System.out.println("row = " + new String(r.getRow()));
			for (TColumnValue resultColumnValue : r.getColumnValues()) {
				System.out.println("family = "
						+ new String(resultColumnValue.getFamily()));
				System.out.println("qualifier = "
						+ new String(resultColumnValue.getQualifier()));
				System.out.println("value = "
						+ unicodeToUtf8(Bytes.toString(resultColumnValue.getValue())));
				// + new String(resultColumnValue.getValue()));
				System.out.println("timestamp = "
						+ resultColumnValue.getTimestamp());
			}
		}

		transport.close();
		
		
		//multiple put
		List<TPut> puts = new ArrayList<TPut>();
		TPut put = new TPut();
		put.setRow("row1".getBytes());

		TColumnValue columnValue = new TColumnValue();
		columnValue.setFamily("family1".getBytes());
		columnValue.setQualifier("qualifier1".getBytes());
		columnValue.setValue("value1".getBytes());
		columnValue.setTimestamp(137090805234L);
		List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
		columnValues.add(columnValue);
		put.setColumnValues(columnValues);
		
		puts.add(put);
		
		TPut put2 = new TPut();
		put2.setRow("row2".getBytes());

		TColumnValue columnValue2 = new TColumnValue();
		columnValue2.setFamily("family2".getBytes());
		columnValue2.setQualifier("qualifier2".getBytes());
		columnValue2.setValue("value2".getBytes());
		columnValue2.setTimestamp(13712312312343L);
		List<TColumnValue> columnValues2 = new ArrayList<TColumnValue>();
		columnValues2.add(columnValue2);
		put2.setColumnValues(columnValues2);
		
		puts.add(put2);
		client.putMultiple(table, puts);
	}

	/**
	 * utf8 -> unicode
	 * 
	 * @param inStr
	 * @return String
	 */
	public static String utf8ToUnicode(String inStr) {
		char[] myBuffer = inStr.toCharArray();

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < inStr.length(); i++) {
			UnicodeBlock ub = UnicodeBlock.of(myBuffer[i]);
			if (ub == UnicodeBlock.BASIC_LATIN) {
				sb.append(myBuffer[i]);
			} else if (ub == UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
				int j = (int) myBuffer[i] - 65248;
				sb.append((char) j);
			} else {
				short s = (short) myBuffer[i];
				String hexS = Integer.toHexString(s);
				String unicode = "\\u" + hexS;
				sb.append(unicode.toLowerCase());
			}
		}
		return sb.toString();
	}

	/**
	 * unicode -> utf8
	 * @param theString
	 * @return String
	 */
	public static String unicodeToUtf8(String theString) {
		char aChar;
		int len = theString.length();
		StringBuffer outBuffer = new StringBuffer(len);
		for (int x = 0; x < len;) {
			aChar = theString.charAt(x++);
			if (aChar == '\\') {
				aChar = theString.charAt(x++);
				if (aChar == 'u') {
					// Read the xxxx
					int value = 0;
					for (int i = 0; i < 4; i++) {
						aChar = theString.charAt(x++);
						switch (aChar) {
						case '0':
						case '1':
						case '2':
						case '3':
						case '4':
						case '5':
						case '6':
						case '7':
						case '8':
						case '9':
							value = (value << 4) + aChar - '0';
							break;
						case 'a':
						case 'b':
						case 'c':
						case 'd':
						case 'e':
						case 'f':
							value = (value << 4) + 10 + aChar - 'a';
							break;
						case 'A':
						case 'B':
						case 'C':
						case 'D':
						case 'E':
						case 'F':
							value = (value << 4) + 10 + aChar - 'A';
							break;
						default:
							throw new IllegalArgumentException(
									"Malformed   \\uxxxx   encoding.");
						}
					}
					outBuffer.append((char) value);
				} else {
					if (aChar == 't')
						aChar = '\t';
					else if (aChar == 'r')
						aChar = '\r';
					else if (aChar == 'n')
						aChar = '\n';
					else if (aChar == 'f')
						aChar = '\f';
					outBuffer.append(aChar);
				}
			} else
				outBuffer.append(aChar);
		}
		return outBuffer.toString();
	}

}