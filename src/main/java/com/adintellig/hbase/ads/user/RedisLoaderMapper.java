package com.adintellig.hbase.ads.user;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.adintellig.util.ConfigFactory;
import com.adintellig.util.ConfigProperties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisLoaderMapper extends Mapper<LongWritable, Text, Text, Text> {
	static ConfigProperties config = ConfigFactory.getInstance()
			.getConfigProperties("/app.properties");
	private String host;
	private int port;
	private String pwd;
	private JedisPool pool;
	private Jedis jedis;
	private String field;

	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		this.host = config.getProperty("redis.host");
		this.port = config.getInt("redis.port", 6379);
		this.pwd = config.getProperty("redis.pwd");

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(1000);
		config.setMaxIdle(20);
		this.pool = new JedisPool(config, this.host, this.port, 20000);
		this.jedis = ((Jedis) this.pool.getResource());
		if (null != pwd && pwd.trim().length() > 0)
			this.jedis.auth(pwd.trim());

		this.field = context.getConfiguration().get("conf.field");
	}

	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String[] arr = valueStr.split("\t", -1);

		if (arr.length == 2)
			try {
				this.jedis.hset(arr[0], this.field, arr[1]);
				context.getCounter("advindex-job1", "redis load record number")
						.increment(1L);

				if (null != this.jedis)
					this.pool.returnResource(this.jedis);
			} catch (Exception e) {
				e.printStackTrace();

				if (null != this.jedis)
					this.pool.returnResource(this.jedis);
			} finally {
				if (null != this.jedis)
					this.pool.returnResource(this.jedis);
			}
	}

	protected void cleanup(
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		this.pool.destroy();
	}
}