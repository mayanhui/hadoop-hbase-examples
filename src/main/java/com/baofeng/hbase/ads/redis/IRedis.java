package com.baofeng.hbase.ads.redis;

import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;

public interface IRedis {
	public void close();
	public void reConnect();
	public Object popBeanForByteArray(String queueName) ;
	public boolean clearDB();
	/**
	 * 将对象放入"BeanQueue"队列中
	 * 
	 * @param <T>
	 * @param queueName
	 * @param t
	 */
	public <T extends Serializable> void putBeantoQueue(T t,String queueName);
	/**
	 * 从队列中读取Bean对象
	 * 
	 * @return
	 */
	public Object popBean(String queueName)throws SocketTimeoutException ;
	public boolean existsKey(String key);
	public boolean setKey(String key,byte[] valueArray);
	/**
	 * 批量写入
	 * @param map
	 * @return
	 */
	public boolean setBatchKey(Map<String,byte[]> map);
	/**
	 * 批量读取
	 * @param arrayKey
	 * @return
	 */
	public List<byte[]> getBatchValues(String[] arrayKey);
	public long incrKey(String key);
	public long getSizeForSet();
	public long getSizeForQueue(String queueName);
	
}
