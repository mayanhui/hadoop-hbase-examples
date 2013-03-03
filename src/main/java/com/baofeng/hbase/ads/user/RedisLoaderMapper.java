package com.baofeng.hbase.ads.user;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.baofeng.util.ConfigFactory;
import com.baofeng.util.ConfigProperties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisLoaderMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static final String JEDIS_PASSWORD = "_houyi630";
	static ConfigProperties config = ConfigFactory.getInstance()
			.getConfigProperties(ConfigFactory.APP_CONFIG_PATH);

	private String host;
	private int port;
	private JedisPool pool;
	private Jedis jedis;

	private String field;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		host = config.getProperty("redis.host");
		port = config.getInt("redis.port", 6379);

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(1000);
		config.setMaxIdle(20);
		pool = new JedisPool(config, host, port, 20000);
		jedis = pool.getResource();
		jedis.auth(JEDIS_PASSWORD);

		field = context.getConfiguration().get("conf.field");

	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String[] arr = valueStr.split("\t", -1);

		if (arr.length == 2) {
			try {
				jedis.hset(arr[0], field, arr[1]);
				context.getCounter("advindex-job1", "redis load record number")
						.increment(1);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (null != jedis)
					pool.returnResource(jedis);
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		pool.destroy();
	}
}
