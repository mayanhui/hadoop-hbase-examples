package com.baofeng.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormatUtil {
	public static final String FROMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String FROMAT_2 = "yyyyMMdd";

	public static long formatStringTimeToLong(String timeLine) {
		long time = -1L;
		SimpleDateFormat format = new SimpleDateFormat(FROMAT);
		try {
			time = format.parse(timeLine).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return time;
	}

	public static long formatStringTimeToLong2(String timeLine) {
		long time = -1L;
		SimpleDateFormat format = new SimpleDateFormat(FROMAT_2);
		try {
			time = format.parse(timeLine).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return time;
	}
	
	public static String parseToStringDate(long ms) {
		SimpleDateFormat format = new SimpleDateFormat(FROMAT);
		String time = format.format(new Date(ms));
		return time;
	}

	public static String parseToStringDate2(long ms) {
		SimpleDateFormat format = new SimpleDateFormat(FROMAT_2);
		String time = format.format(new Date(ms));
		return time;
	}

	@SuppressWarnings("deprecation")
	public static String getLastHour() {
		String hour = null;
		Date d = new Date(System.currentTimeMillis() - 3600 * 1000);
		hour = String.valueOf(d.getHours());
		if (Integer.parseInt(hour) < 10) {
			hour = "0" + hour;
		}
		return hour;
	}

	public static void main(String[] args) {
		String str = "2013-01-15 17:54:29 345:asdf";
		System.out.println(str);
		System.out.println(formatStringTimeToLong(str));
		System.out.println(Integer.parseInt("20130121")
				- Integer.parseInt("20121121"));
		String s = parseToStringDate(formatStringTimeToLong(str));
		System.out.println(s);
		System.out.println(parseToStringDate(1358506007000L));
		System.out.println(getLastHour());
		str = "20130302";
		System.out.println(formatStringTimeToLong2(str));
		Long t = 1369826004957l;
		System.out.println(parseToStringDate(t));
	}
}
