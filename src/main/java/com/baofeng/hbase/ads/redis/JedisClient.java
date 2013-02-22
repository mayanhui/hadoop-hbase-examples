package com.baofeng.hbase.ads.redis;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisClient implements IRedis {
	private String pass;
	private int dbNum;
	private static JedisPool pool = null;
	private Jedis jedis;
	private String ip;
	private int port;

	public JedisClient(String[] jedisArray) {
		this(jedisArray[0], Integer.parseInt(jedisArray[1]), jedisArray[2],
				Integer.parseInt(jedisArray[3]));
	}

	public String getIpAndPort() {
		return ip + ":" + port;
	}

	public JedisClient(String ip, int port, String pass, int dbNum) {
		if (pool == null) {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxActive(1000);
			config.setMaxIdle(20);
			pool = new JedisPool(config, ip, port, 20000);
		}

		jedis = pool.getResource();

		this.pass = pass;
		this.dbNum = dbNum;
		// jedis.auth(pass);
		// jedis.select(dbNum);
	}

	public void reInitClient() {
		this.init(ip, port);
	}

	public void init(String ip, int port) {
		this.ip = ip;
		this.port = port;
		pool.returnBrokenResource(jedis);
		jedis = null;
		jedis = pool.getResource();
		jedis.auth(pass);
		jedis.select(dbNum);
	}

	public static void main(String[] args) throws Exception {
		String ip = "192.168.85.210";
		ip = "192.168.1.111";
		int port = 6379;
		
		JedisClient client = new JedisClient(ip, port, null, 1);

		long st = System.currentTimeMillis();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream("/root/part-r-00000")));
		String line = null;
		int count = 0;
		while (null != (line = br.readLine())) {
			++count;
			line = line.trim();
			String[] arr = line.split("\t", -1);
			if (arr.length == 2) {
				String uid = arr[0];
				String adidOrAttr = arr[1];
				client.putBeantoQueue(adidOrAttr, uid);
			}
			if (count % 2000 == 0) {
				System.out.println("count: " + count);
			}
		}

		long en = System.currentTimeMillis();
		System.out.println("count: " + count);
		System.out.println("time: " + (en - st));
		System.out.println("AVG time: " + (en - st) / (double) count);
		br.close();

		// try {
		// Vector v = new Vector();
		// v.add("{0E9F6D99-5330-06EA-0C9C-F20D15AA75C7}\t27823\t13\t1788\t3861\t2012-06-08 14:01:59\t0");
		// client.putBeantoQueue(v, "output2");
		// Object obj = client.popBean("2409935700744716621");
		// System.out.println(obj);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// for (int i = 0; i < 1000; i++) {
		// System.out.println(client.get("movieId:100829"));
		// }

		// try {
		// Object obj = client.popBean("my_redis");
		// System.out.println(obj);
		// } catch (SocketTimeoutException e) {
		// e.printStackTrace();
		// }

		// while(true){
		// try {
		// List<String> list = client.getBatchKey(new String[]{"11"});
		// for(String s:list){
		// System.out.println(Integer.parseInt(s));
		// }
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }

	}

	/**
	 * 设置Redis里key对应的value值（为对象）
	 * 
	 * @param <T>
	 * @param key
	 * @param t
	 */
	public <T extends Serializable> void setKeyValueForByte(String key, byte[] t) {
		try {
			jedis.set(key.getBytes("utf-8"), t);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public String get(String key) {
		return jedis.get(key);
	}

	public Object getKeyValueForByte(String key) {
		try {
			byte[] _array = jedis.get(key.getBytes("utf-8"));
			if (_array != null) {
				return _array;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println("对象取出失败");
		return null;
	}

	public void close() {
		pool.returnResource(jedis);
	}

	@Override
	public boolean clearDB() {
		if (jedis.flushDB() != null)
			return true;
		else
			return false;
	}

	@Override
	public boolean existsKey(String key) {
		return jedis.exists(key);
	}

	@Override
	public List<byte[]> getBatchValues(String[] arrayKey) {
		return null;
	}

	public long incr(String key, long integer) {
		try {
			return jedis.incrBy(key, integer);
		} catch (JedisConnectionException ex) {
			this.reInitClient();
			return jedis.incrBy(key, integer);
		}

	}

	public long decr(String key, long integer) {
		try {
			return jedis.decrBy(key, integer);
		} catch (JedisConnectionException ex) {
			this.reInitClient();
			return jedis.decrBy(key, integer);
		}

	}

	@Override
	public long getSizeForQueue(String queueName) {
		long size = jedis.llen(queueName);
		return size;
	}

	@Override
	public long getSizeForSet() {
		long size;

		size = jedis.dbSize();

		return size;
	}

	/**
	 * 需要将每一个元素强制转换至 long 或者 int
	 * 
	 * @param keys
	 * @return
	 */
	public List<String> getBatchKey(String[] keys) {
		return jedis.mget(keys);
	}

	@Override
	public long incrKey(String key) {

		return jedis.incr(key);

	}

	@Override
	public Object popBeanForByteArray(String queueName) {
		byte[] bt = null;
		try {
			bt = jedis.rpop(queueName.getBytes("utf-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		} catch (Exception ex) {
			return null;
		}
		if (bt != null)
			return bt;
		else
			return null;

	}

	/*
	 * private Object hessianToObj(byte[] bytes) throws IOException,
	 * ClassNotFoundException { ByteArrayInputStream is = new
	 * ByteArrayInputStream(bytes); Hessian2Input in = new Hessian2Input(is);
	 * in.setSerializerFactory(factory); in.read();//"r" in.read();//>=2
	 * in.read();//0 Object value = in.readObject(); is.close(); return value; }
	 * 
	 * private byte[] objToHessian( Object object) throws IOException {
	 * ByteArrayOutputStream os = new ByteArrayOutputStream();
	 * AbstractHessianOutput out = new Hessian2Output(os);
	 * out.setSerializerFactory(factory); out.startReply();
	 * out.writeObject(object); out.completeReply(); out.flush(); byte[] array =
	 * os.toByteArray(); os.close(); os = null; out = null; return array; }
	 */
	@Override
	public Object popBean(String queueName) throws SocketTimeoutException {
		try {
			Object obj = popBeanForByteArray(queueName);
			if (obj == null)
				return null;
			else {
				byte[] array = (byte[]) obj;
				// return this.hessianToObj(array);
				ByteArrayInputStream in = new ByteArrayInputStream(array);
				ObjectInputStream sIn = new ObjectInputStream(in);
				return sIn.readObject();
			}
		} catch (JedisConnectionException ex) {
			System.out
					.println("### server closed idle socket ready to reconnect..###");
			this.reInitClient();
			return null;
		} catch (SocketTimeoutException ex) {
			System.out
					.println("### server closed idle socket ready to reconnect..###");
			this.reInitClient();
			return null;
		} catch (java.net.SocketException ex) {
			System.out
					.println("### server closed idle socket ready to reconnect..###");
			this.reInitClient();
			return null;
		} catch (Exception e) {
			System.out.println(e.getMessage()
					+ "### server closed idle socket ready to reconnect..###");
			this.reInitClient();
			e.printStackTrace();
			return null;
		}
	}

	public <T extends Serializable> void putBeantoQueue(T t,
			String[] queueNameArray) {
		for (String queue : queueNameArray) {
			this.putBeantoQueue(t, queue);
		}
	}

	@Override
	public <T extends Serializable> void putBeantoQueue(T t, String queueName) {
		/*
		 * try { byte[] bytes = this.objToHessian(t);
		 * jedis.lpush(queueName.getBytes("utf-8"), bytes); }catch (IOException
		 * ex){ System.out.println("### 序列化对象失败 ###"); }
		 */
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] array = null;
		try {
			ObjectOutputStream obj = new ObjectOutputStream(out);
			obj.writeObject(t);
			array = out.toByteArray();

			// jedis.lpush(queueName.getBytes("utf-8"), array);
			jedis.hset(queueName.getBytes(), "adidlist".getBytes(), array);

			out.close();
			out = null;
			obj.close();
			obj = null;
		} catch (JedisConnectionException ex) {
			System.out
					.println("### server closed idle socket ready to reconnect..###");
			this.reInitClient();
		} catch (SocketTimeoutException ex) {
			System.out
					.println("### server closed idle socket ready to reconnect..###");
			this.reInitClient();

		} catch (java.net.SocketException ex) {
			System.out
					.println("### server closed idle socket ready to reconnect..###");
			this.reInitClient();

		} catch (IOException e1) {
			System.out
					.println("### server closed idle socket ready to reconnect..###");
			this.reInitClient();
		} catch (Exception e) {
			System.out.println(e.getMessage()
					+ "### server closed idle socket ready to reconnect..###");
			this.reInitClient();
			e.printStackTrace();
		}
	}

	@Override
	public boolean setBatchKey(Map<String, byte[]> map) {
		for (String key : map.keySet()) {
			try {
				jedis.set(key.replaceAll(" ", "_").getBytes("utf-8"),
						map.get(key));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		HashMap<String, byte[]> newMap = new HashMap<String, byte[]>();
		for (String key : map.keySet()) {
			newMap.put(key.replaceAll(" ", "_"), map.get(key));
		}
		// jedis.mset(keysvalues)
		return true;
	}

	@Override
	public boolean setKey(String key, byte[] valueArray) {
		try {
			jedis.set(key.getBytes("utf-8"), valueArray);
			return true;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void reConnect() {
		pool.returnBrokenResource(jedis);
	}

}
