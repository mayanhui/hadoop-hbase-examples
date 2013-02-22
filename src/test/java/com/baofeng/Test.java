package com.baofeng;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Test {

	public static void main(String[] args) {
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
}
