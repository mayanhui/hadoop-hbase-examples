package com.adintellig.interview;

import java.util.*;

/**
 * 有一个Hashmap<String,String> a 对一个给定的value = 12，遍历该Hashmap，找到该value，
 * 并打印出对应的key，value？
 * 
 */
public class TraverseHashMap {
	public static void main(String[] args) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("k12", "12");
		map.put("k13", "13");

		Set<Map.Entry<String, String>> set = map.entrySet();
		for (Map.Entry<String, String> entry : set) {
			String value = entry.getValue();
			if (value.equals("12")) {
				System.out.println(entry.getKey() + "=>" + value);
			}
		}
	}
}
