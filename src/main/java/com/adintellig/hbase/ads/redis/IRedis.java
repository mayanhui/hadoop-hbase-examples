package com.adintellig.hbase.ads.redis;

import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;

public abstract interface IRedis
{
  public abstract void close();

  public abstract void reConnect();

  public abstract Object popBeanForByteArray(String paramString);

  public abstract boolean clearDB();

  public abstract <T extends Serializable> void putBeantoQueue(T paramT, String paramString);

  public abstract Object popBean(String paramString)
    throws SocketTimeoutException;

  public abstract boolean existsKey(String paramString);

  public abstract boolean setKey(String paramString, byte[] paramArrayOfByte);

  public abstract boolean setBatchKey(Map<String, byte[]> paramMap);

  public abstract List<byte[]> getBatchValues(String[] paramArrayOfString);

  public abstract long incrKey(String paramString);

  public abstract long getSizeForSet();

  public abstract long getSizeForQueue(String paramString);
}