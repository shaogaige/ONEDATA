/**
 * ClassName:RedisClusterUtils.java
 * Date:2019年7月31日
 */

package com.idata.tool;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import redis.clients.jedis.JedisPoolConfig;

/**
 * Create:HE ZhiGao
 * Description:
 * Log:
 */
public class RedisClusterUtils{
	
	private static JedisCluster jedis;
    static {
    	String string="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006";
        // 添加集群的服务节点Set集合
        Set<HostAndPort> hostAndPortsSet = new HashSet<HostAndPort>();
        String[] split = string.split(",");        
        // 添加节点
        for (String string2 : split)
		{
        	String[] split2 = string2.split(":");
        	hostAndPortsSet.add(new HostAndPort(split2[0], Integer.parseInt(split2[1])));
		}
        // Jedis连接池配置
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 最大空闲连接数, 默认8个
        jedisPoolConfig.setMaxIdle(100);
        // 最大连接数, 默认8个
        jedisPoolConfig.setMaxTotal(500);
        //最小空闲连接数, 默认0
        jedisPoolConfig.setMinIdle(0);
        // 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
        // 设置2秒
        jedisPoolConfig.setMaxWaitMillis(2000); 
        //对拿到的connection进行validateObject校验
        jedisPoolConfig.setTestOnBorrow(true);
        jedis = new JedisCluster(hostAndPortsSet, jedisPoolConfig);
        //jedis = new JedisCluster(hostAndPortsSet, 1, 1, 1, "面", jedisPoolConfig);
        //jedis.auth("密码");
    }
                      
    public JedisCluster getJedisCluster() {
    	return jedis;
    }
    //-----------------------------key:value数据----------------------------
   /**
    * 如果 key 已经存储其他值， SET 就覆写旧值，且无视类型。
    * @param key
    * @param value
    * @return
    */
   public String set(String key,String value) {
	   return jedis.set(key, value);
   }
 
   
   /**
    * 如果 key 已经存储其他值， SET 就覆写旧值，且无视类型。
    * @param key
    * @param value
    * @return
    */
   public String set(byte[]  key,byte[] value) {
	   return jedis.set(key, value);
   }
   /**
    * 在指定的 key 不存在时，为 key 设置指定的值。
    * @param key
    * @return
    */
   public Long setnx(String  key,String value) {
	   return jedis.setnx(key, value);
   }
   /**
    * 在指定的 key 不存在时，为 key 设置指定的值。
    * @param key
    * @param value
    * @return
    */
   public Long setnx(byte[]  key,byte[] value) {
	   return jedis.setnx(key, value);
   }
   /**
    *  命令为指定的 key 设置值及其过期时间。如果 key 已经存在， SETEX 命令将会替换旧的值
    * @param key
    * @param seconds 秒
    * @param value
    * @return
    */
   public String setex(String  key,int seconds,String value) {
	   return jedis.setex(key, seconds, value);
   }
   /**
    *  命令为指定的 key 设置值及其过期时间。如果 key 已经存在， SETEX 命令将会替换旧的值
    * @param key
    * @param seconds 秒
    * @param value
    * @return
    */
   public String setex(byte[]  key,int seconds,byte[] value) {
	   return jedis.setex(key, seconds, value);
   }
   /**
    *  命令为指定的 key 设置值及其过期时间。如果 key 已经存在， SETEX 命令将会替换旧的值
    * @param key
    * @param seconds毫秒
    * @param value
    * @return
    */
   public String psetex(String  key,Long seconds,String value) {
	   return jedis.psetex(key, seconds, value);
   }

   /**
    * 设置key的超时时间为seconds
    * @param key
    * @param seconds  单位秒
    * @return
    */
   public Long expire(byte[]  key,int seconds) {
	   return jedis.expire(key, seconds);
   }
   /**
    * 设置key的超时时间为seconds
    * @param key
    * @param seconds 单位秒
    * @return
    */
   public Long expire(String key,int seconds) {
	   return jedis.expire(key, seconds);
   }
  
   /**
    * 设置key的超时时间为seconds
    * @param key
    * @param seconds  单位毫秒
    * @return
    */
   public Long pexpire(byte[]  key,Long seconds) {
	   return jedis.pexpire(key, seconds);
   }
   /**
    * 设置key的超时时间为seconds
    * @param key
    * @param seconds 单位毫秒
    * @return
    */
   public Long pexpire(String key,Long seconds) {
	   return jedis.pexpire(key, seconds);
   }
   
   //--------------------------------------------------------------------------------------
   /**
    * 命令用于获取指定 key 的值。如果 key 不存在，返回 nil 。如果key 储存的值不是字符串类型，返回一个错误
    * @param key
    * @return
    */
   public String get(String key) {
	   return jedis.get(key);
   }
   
   
   /**
    * 命令用于获取指定 key 的值。如果 key 不存在，返回 nil 。如果key 储存的值不是字符串类型，返回一个错误
    * @param key
    * @return
    */
   public byte[]  get(byte[]  key) {
	   return jedis.get(key);
   }
   
   /**
    * 命令返回所有(一个或多个)给定 key 的值。 如果给定的 key 里面，有某个 key 不存在，那么这个 key 返回特殊值 nil
    * @param key
    * @return
    */
   public List<String> mget(String key) {
	    return jedis.mget(key);
   }
   /**
    * 命令返回所有(一个或多个)给定 key 的值。 如果给定的 key 里面，有某个 key 不存在，那么这个 key 返回特殊值 nil
    * @param key
    * @return
    */
   public List<byte[]>  mget(byte[]  key) {
	   return jedis.mget(key);
   }
   
   
   
   

}
