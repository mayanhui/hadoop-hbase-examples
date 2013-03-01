package com.baofeng;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import com.baofeng.advindex.Const;
import com.baofeng.util.DateFormatUtil;

//import com.baofeng.util.DateFormatUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Test {

	public static void main1(String[] args) {
		JedisPool pool = new JedisPool(new JedisPoolConfig(), "192.168.85.210");
		Jedis jedis = pool.getResource();
		try {
			// / ... do stuff here ... for example
			jedis.set("foo", "bar");
			String foobar = jedis.get("foo");
			System.out.println(foobar);

			jedis.zadd("sose", 0, "car");
			jedis.zadd("sose", 0, "bike");
			Set<String> sose = jedis.zrange("sose", 0, -1);
			for (String s : sose) {
				System.out.println(s);
			}

			jedis.hset("hash1", "f1", "!@#$%^&*");
			String f = jedis.hget("hash1", "f1");
			System.out.println(f);

			f = jedis.hget("8430732852077608528", "adidlist").toString();
			jedis.hgetAll("8430732852077608528");
			System.out.println(jedis.hgetAll("8430732852077608528"));

		} finally {
			// / ... it's important to return the Jedis instance to the pool
			// once you've finished using it
			pool.returnResource(jedis);
		}
		pool.destroy();
	}

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream("/root/vv.txt")));
		String line = null;
		while (null != (line = br.readLine())) {
			String[] arr = line.split("\t", -1);
			String uid = null;
			String type = null;

			String aid = null;
			String wid = null;

			String mname = null;
			if (arr.length == 31) {// vv 31 fields
				uid = arr[6].trim();

				aid = arr[8].trim();
				wid = arr[9].trim();
				type = arr[11].trim();

				if (null != type && type.equals("2")) {// 成功vv
					if (null != aid && null != wid && null != uid
							&& aid.length() > 0 && wid.length() > 0
							&& uid.length() > 0) {
						System.out.println(uid + "|" + aid + "|" + wid + "|" + type);
					}
				}
			}
		}
		br.close();
		
		String s = DateFormatUtil.parseToStringDate(1359790111000L);
		System.out.println(s);
	}

	private static String generateMonthInput(String input, String date) {
		int m = 30;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < m; i++) {
			sb.append(input + File.separator + getDateByDay(date, i) + ",");
		}

		if (sb.length() > 0)
			sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	public static String getDateByDay(String dateStr, int day) {
		Calendar date = Calendar.getInstance();
		date.setTime(new Date(formatStringTimeToLong2("20130224")));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		date.add(Calendar.DATE, -day);
		return sdf.format(date.getTime());
	}

	public static long formatStringTimeToLong2(String timeLine) {
		long time = -1L;
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		try {
			time = format.parse(timeLine).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return time;
	}
}
