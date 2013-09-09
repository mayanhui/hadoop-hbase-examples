package com.adintellig.hbase.ads.redis;

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
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisClient
  implements IRedis
{
  private String pass;
  private int dbNum;
  private static JedisPool pool = null;
  private Jedis jedis;
  private String ip;
  private int port;

  public JedisClient(String[] jedisArray)
  {
    this(jedisArray[0], Integer.parseInt(jedisArray[1]), jedisArray[2], Integer.parseInt(jedisArray[3]));
  }

  public String getIpAndPort()
  {
    return this.ip + ":" + this.port;
  }

  public JedisClient(String ip, int port, String pass, int dbNum) {
    if (pool == null) {
      JedisPoolConfig config = new JedisPoolConfig();
      config.setMaxActive(1000);
      config.setMaxIdle(20);
      pool = new JedisPool(config, ip, port, 20000);
    }

    this.jedis = ((Jedis)pool.getResource());

    this.pass = pass;
    this.dbNum = dbNum;
  }

  public void reInitClient()
  {
    init(this.ip, this.port);
  }

  public void init(String ip, int port) {
    this.ip = ip;
    this.port = port;
    pool.returnBrokenResource(this.jedis);
    this.jedis = null;
    this.jedis = ((Jedis)pool.getResource());
    this.jedis.auth(this.pass);
    this.jedis.select(this.dbNum);
  }

  public static void main(String[] args) throws Exception {
    String ip = "192.168.85.210";
    ip = "192.168.1.111";
    ip = "114.112.82.107 ";
    int port = 6379;
    port = 9736;

    JedisClient client = new JedisClient(ip, port, null, 1);
    
    String value = client.get("{00007C20-94E3-4178-0AA7-234561491C75}");
    long i = client.getSizeForSet();
    System.out.println("value: " + i);
//    long st = System.currentTimeMillis();
//    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/root/part-r-00000")));
//
//    String line = null;
//    int count = 0;
//    while (null != (line = br.readLine())) {
//      count++;
//      line = line.trim();
//      String[] arr = line.split("\t", -1);
//      if (arr.length == 2) {
//        String uid = arr[0];
//        String adidOrAttr = arr[1];
//        client.putBeantoQueue(adidOrAttr, uid);
//      }
//      if (count % 2000 == 0) {
//        System.out.println("count: " + count);
//      }
//    }
//
//    long en = System.currentTimeMillis();
//    System.out.println("count: " + count);
//    System.out.println("time: " + (en - st));
//    System.out.println("AVG time: " + (en - st) / count);
//    br.close();
  }

  public <T extends Serializable> void setKeyValueForByte(String key, byte[] t)
  {
    try
    {
      this.jedis.set(key.getBytes("utf-8"), t);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  public String get(String key) {
    return this.jedis.get(key);
  }

  public Object getKeyValueForByte(String key) {
    try {
      byte[] _array = this.jedis.get(key.getBytes("utf-8"));
      if (_array != null)
        return _array;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    return null;
  }

  public void close() {
    pool.returnResource(this.jedis);
  }

  public boolean clearDB()
  {
    return this.jedis.flushDB() != null;
  }

  public boolean existsKey(String key)
  {
    return this.jedis.exists(key).booleanValue();
  }

  public List<byte[]> getBatchValues(String[] arrayKey)
  {
    return null;
  }

  public long incr(String key, long integer) {
    try {
      return this.jedis.incrBy(key, integer).longValue();
    } catch (JedisConnectionException ex) {
      reInitClient();
    }return this.jedis.incrBy(key, integer).longValue();
  }

  public long decr(String key, long integer)
  {
    try
    {
      return this.jedis.decrBy(key, integer).longValue();
    } catch (JedisConnectionException ex) {
      reInitClient();
    }return this.jedis.decrBy(key, integer).longValue();
  }

  public long getSizeForQueue(String queueName)
  {
    long size = this.jedis.llen(queueName).longValue();
    return size;
  }

  public long getSizeForSet()
  {
    long size = this.jedis.dbSize().longValue();

    return size;
  }

  public List<String> getBatchKey(String[] keys)
  {
    return this.jedis.mget(keys);
  }

  public long incrKey(String key)
  {
    return this.jedis.incr(key).longValue();
  }

  public Object popBeanForByteArray(String queueName)
  {
    byte[] bt = null;
    try {
      bt = this.jedis.rpop(queueName.getBytes("utf-8"));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return null;
    } catch (Exception ex) {
      return null;
    }
    if (bt != null) {
      return bt;
    }
    return null;
  }

  public Object popBean(String queueName)
    throws SocketTimeoutException
  {
    try
    {
      Object obj = popBeanForByteArray(queueName);
      if (obj == null) {
        return null;
      }
      byte[] array = (byte[])(byte[])obj;

      ByteArrayInputStream in = new ByteArrayInputStream(array);
      ObjectInputStream sIn = new ObjectInputStream(in);
      return sIn.readObject();
    }
    catch (JedisConnectionException ex) {
      System.out.println("### server closed idle socket ready to reconnect..###");

      reInitClient();
      return null;
    } catch (SocketTimeoutException ex) {
      System.out.println("### server closed idle socket ready to reconnect..###");

      reInitClient();
      return null;
    } catch (SocketException ex) {
      System.out.println("### server closed idle socket ready to reconnect..###");

      reInitClient();
      return null;
    } catch (Exception e) {
      System.out.println(e.getMessage() + "### server closed idle socket ready to reconnect..###");

      reInitClient();
      e.printStackTrace();
    }return null;
  }

  public <T extends Serializable> void putBeantoQueue(T t, String[] queueNameArray)
  {
    for (String queue : queueNameArray)
      putBeantoQueue(t, queue);
  }

  public <T extends Serializable> void putBeantoQueue(T t, String queueName)
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] array = null;
    try {
      ObjectOutputStream obj = new ObjectOutputStream(out);
      obj.writeObject(t);
      array = out.toByteArray();

      this.jedis.hset(queueName.getBytes(), "adidlist".getBytes(), array);

      out.close();
      out = null;
      obj.close();
      obj = null;
    } catch (JedisConnectionException ex) {
      System.out.println("### server closed idle socket ready to reconnect..###");

      reInitClient();
    } catch (SocketTimeoutException ex) {
      System.out.println("### server closed idle socket ready to reconnect..###");

      reInitClient();
    }
    catch (SocketException ex) {
      System.out.println("### server closed idle socket ready to reconnect..###");

      reInitClient();
    }
    catch (IOException e1) {
      System.out.println("### server closed idle socket ready to reconnect..###");

      reInitClient();
    } catch (Exception e) {
      System.out.println(e.getMessage() + "### server closed idle socket ready to reconnect..###");

      reInitClient();
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unchecked")
public boolean setBatchKey(Map<String, byte[]> map)
  {
    for (String key : map.keySet()) {
      try {
        this.jedis.set(key.replaceAll(" ", "_").getBytes("utf-8"), (byte[])map.get(key));
      }
      catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
    @SuppressWarnings("rawtypes")
	HashMap newMap = new HashMap();
    for (String key : map.keySet()) {
      newMap.put(key.replaceAll(" ", "_"), map.get(key));
    }

    return true;
  }

  public boolean setKey(String key, byte[] valueArray)
  {
    try {
      this.jedis.set(key.getBytes("utf-8"), valueArray);
      return true;
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }return false;
  }

  public void reConnect()
  {
    pool.returnBrokenResource(this.jedis);
  }
}