package com.baofeng.util;

import java.io.File;

public class FileUtil {

	public static boolean deleteFile(File f) {
		if (f.exists()) {
			if (f.isFile())
				return f.delete();
			else if (f.isDirectory()) {
				File[] files = f.listFiles();
				for (int i = 0; i < files.length; i++) {
					if (!deleteFile(files[i]))
						return false;
				}
				return f.delete();
			} else
				return false;
		} else
			return false;
	}

	public static void main(String[] args) {
		String file = "/tmp/attribute_ads";
		FileUtil.deleteFile(new File(file));
	}

}
