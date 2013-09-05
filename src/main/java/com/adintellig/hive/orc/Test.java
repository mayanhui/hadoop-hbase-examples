package com.adintellig.hive.orc;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Test {
	Path workDir = new Path(System.getProperty("test.tmp.dir",
			"target/test/tmp"));
	JobConf conf;
	FileSystem fs;
	Path testFilePath;

	public void testMROutput() throws Exception {
		JobConf conf = new JobConf();
		fs = FileSystem.getLocal(conf);
		testFilePath = new Path(workDir, "TestInputOutputFormat.1" + ".orc");
		fs.delete(testFilePath, false);
		
		//WRITE
		Properties properties = new Properties();
		StructObjectInspector inspector;
		inspector = (StructObjectInspector) ObjectInspectorFactory
				.getReflectionObjectInspector(NestedRow.class,
						ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		SerDe serde = new OrcSerde();
		OutputFormat<?, ?> outFormat = new OrcOutputFormat();
		
		RecordWriter writer = outFormat.getRecordWriter(fs, conf,
				testFilePath.toString(), Reporter.NULL);
		writer.write(NullWritable.get(),
				serde.serialize(new NestedRow(1, 2, 3), inspector));
		writer.write(NullWritable.get(),
				serde.serialize(new NestedRow(4, 5, 6), inspector));
		writer.write(NullWritable.get(),
				serde.serialize(new NestedRow(7, 8, 9), inspector));
		writer.close(Reporter.NULL);
		
		
		
		
		
		//READ
		serde = new OrcSerde();
		properties.setProperty("columns", "z,r");
		properties.setProperty("columns.types", "int:struct<x:int,y:int>");
		serde.initialize(conf, properties);
		inspector = (StructObjectInspector) serde.getObjectInspector();
		InputFormat<?, ?> in = new OrcInputFormat();
		FileInputFormat.setInputPaths(conf, testFilePath.toString());
		InputSplit[] splits = in.getSplits(conf, 1);

		// assertEquals(1, splits.length);
		System.out.println("splits.length=" + splits.length);

		conf.set("hive.io.file.readcolumn.ids", "1");
		org.apache.hadoop.mapred.RecordReader reader = in.getRecordReader(
				splits[0], conf, Reporter.NULL);
		Object key = reader.createKey();
		Object value = reader.createValue();
		int rowNum = 0;
		List<? extends StructField> fields = inspector.getAllStructFieldRefs();
		StructObjectInspector inner = (StructObjectInspector) fields.get(1)
				.getFieldObjectInspector();
		List<? extends StructField> inFields = inner.getAllStructFieldRefs();
		IntObjectInspector intInspector = (IntObjectInspector) fields.get(0)
				.getFieldObjectInspector();

		while (reader.next(key, value)) {
			// assertEquals(null,
			// inspector.getStructFieldData(value, fields.get(0)));
			Object sub = inspector.getStructFieldData(value, fields.get(1));
			// assertEquals(
			// 3 * rowNum + 1,
			// intInspector.get(inner.getStructFieldData(sub,
			// inFields.get(0))));

			System.out.println(intInspector.get(inner.getStructFieldData(sub,
					inFields.get(0))));
			// assertEquals(
			// 3 * rowNum + 2,
			// intInspector.get(inner.getStructFieldData(sub,
			// inFields.get(1))));

			System.out.println(intInspector.get(inner.getStructFieldData(sub,
					inFields.get(1))));
			rowNum += 1;
		}
		// assertEquals(3, rowNum);
		System.out.println(rowNum);
		reader.close();
	}

	public static class MyRow implements Writable {
		int x;
		int y;

		MyRow(int x, int y) {
			this.x = x;
			this.y = y;
		}

		@Override
		public void write(DataOutput dataOutput) throws IOException {
			throw new UnsupportedOperationException("no write");
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			throw new UnsupportedOperationException("no read");
		}
	}

	static class NestedRow implements Writable {
		int z;
		MyRow r;

		NestedRow(int x, int y, int z) {
			this.z = z;
			this.r = new MyRow(x, y);
		}

		@Override
		public void write(DataOutput dataOutput) throws IOException {
			throw new UnsupportedOperationException("unsupported");
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			throw new UnsupportedOperationException("unsupported");
		}
	}

	public static void main(String[] args) throws Exception {
		Test t = new Test();
		t.testMROutput();
	}

}
