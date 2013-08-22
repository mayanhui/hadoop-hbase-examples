package com.adintellig.hbase.userattrlib.cptable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;

/**
 * Split algorithm using a specified file.<br/>
 * Put your splitting keys in {@link #SPLIT_KEY_FILE}, one key per line.
 * 
 */
public class FileSplitAlgorithm implements SplitAlgorithm {

	public static final String SPLIT_KEY_FILE = "split-keys";

	@Override
	public byte[] firstRow() {
		return null;
	}

	@Override
	public byte[] lastRow() {
		return null;
	}

	@Override
	public String rowToStr(byte[] row) {
		return null;
	}

	@Override
	public String separator() {
		return null;
	}

	@Override
	public byte[] split(byte[] start, byte[] end) {
		return null;
	}

	@Override
	public byte[][] split(int numberOfSplits) {
		BufferedReader br = null;
		try {
			File keyFile = new File(SPLIT_KEY_FILE);
			if (!keyFile.exists()) {
				throw new FileNotFoundException("Split key file not found: "
						+ SPLIT_KEY_FILE);
			}

			List<byte[]> regions = new ArrayList<byte[]>();
			br = new BufferedReader(new FileReader(keyFile));
			String line;
			while ((line = br.readLine()) != null) {
				if (line.trim().length() > 0) {
					regions.add(Bytes.toBytes(line));
				}
			}
			return regions.toArray(new byte[0][]);

		} catch (IOException e) {
			throw new RuntimeException("Error reading split keys from "
					+ SPLIT_KEY_FILE, e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
	}

	@Override
	public byte[] strToRow(String input) {
		return null;
	}

	@Override
	public void setFirstRow(String userInput) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLastRow(String userInput) {
		// TODO Auto-generated method stub
		
	}

}
