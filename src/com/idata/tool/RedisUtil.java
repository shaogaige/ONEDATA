/**
 * ClassName:RedisUtil.java
 * Date:2019年7月30日
 */

package com.idata.tool;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Create:HE ZhiGao 
 * Description:操作redis工具类,
 * 如果仅操作一次redis建议直接调用下方工具类;
 * 调用多次redis建议调用getJedis,操作完之后调用jedis.close()手动释放
 *  log:
 */
public class RedisUtil{
	private static  JedisPool pool = null;
	private static  Jedis jedis = null;
	 
	static{
		if (pool == null)
		{
			// 从配置文件中获取配置
			// IP
			String ip = PropertiesUtil.getValue("redis.host");
			// 端口
			String ports = PropertiesUtil.getValue("redis.port");
			int port = Integer.parseInt(ports);
			// 密码
			String password = PropertiesUtil.getValue("redis.password");
			// 创建连接配置
			JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

			// 最大连接数，如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
			String maxTotal = PropertiesUtil.getValue("redis.maxTotal");
			jedisPoolConfig.setMaxTotal(Integer.parseInt(maxTotal));

			// 最大空闲数，控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
			String maxIdle = PropertiesUtil.getValue("redis.maxIdle");
			jedisPoolConfig.setMaxIdle(Integer.parseInt(maxIdle));

			// 最小空闲数
			// String minIdle = PropertiesUtil.getValue("redis.minIdle");
			// jedisPoolConfig.setMinIdle(Integer.parseInt(minIdle));

			// 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
			// String maxWaitMillis = PropertiesUtil.getValue("redis.maxWaitMillis");
			// jedisPoolConfig.setMaxWaitMillis(Long.parseLong(maxWaitMillis));

			// 是否在从池中取出连接前进行检验，如果检验失败，则从池中去除连接并尝试取出另一个
			String testOnBorrow = PropertiesUtil.getValue("redis.testOnBorrow");
			jedisPoolConfig.setTestOnBorrow(Boolean.parseBoolean(testOnBorrow));

			if (password != null && !"".equals(password))
			{
				// redis 设置了密码
				pool = new JedisPool(jedisPoolConfig, ip, port, 10000, password);
			}
			else
			{
				// redis 未设置密码
				pool = new JedisPool(jedisPoolConfig, ip, port, 10000);
			}
		}
	}

	// TODO
	// ------------------------------------字符串操作- redis的key和string类型value限制均为512MB。---------------------------------------------------------
	/**
	 * 获取指定key的值,如果key不存在返回null，如果该Key存储的不是字符串，会抛出一个错误
	 * 
	 * @param key
	 * @return
	 */
	public   String get(String key)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.get(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 添加(如果已存在则覆盖)
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public   String set(String key, String value)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.set(key, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 删除
	 * 
	 * @param keys删除一个时传入string,删除多个时为string[]
	 * @return删除数量
	 */
	public   Long del(String... keys)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.del(keys);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key向指定的value值追加值
	 *
	 * @param key
	 * @param str
	 * @return 返回value总长度
	 */
	public   Long append(String key, String str)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.append(key, str);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 判断key是否存在
	 *
	 * @param key
	 * @return不存在时返回false
	 */
	public   Boolean exists(String key)
	{
		Jedis jedis = getJedis();
		Boolean result = null;
		try
		{
			result = jedis.exists(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;

	}

	/**
	 * 设置key value,如果key已经存在则返回0并不做操作,不存在则添加返回1
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public   Long setnx(String key, String value)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.setnx(key, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 设置key value并指定这个键值的有效期 重复提交可以覆盖上次的值和时间
	 * 
	 * @param key
	 * @param seconds
	 *            单位秒
	 * @param value
	 * @return
	 */
	public   String setex(String key, int seconds, String value)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.setex(key, seconds, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}
	/**
	 * 设置key的超时时间为seconds
	 *
	 * @param key
	 * @param seconds
	 * @return
	 */
	public   Long expire(String key, int seconds)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.expire(key, seconds);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key 和offset 从指定的位置开始将原先value替换
	 *
	 * @param key
	 * @param offset
	 * @param str
	 * @return
	 */
	public   Long setrange(String key, int offset, String str)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.setrange(key, offset, str);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过批量的key获取批量的value
	 *
	 * @param keys
	 * @return
	 */
	public   List<String> mget(String... keys)
	{
		Jedis jedis = getJedis();
		List<String> result = null;
		try
		{
			result = jedis.mget(keys);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 批量的设置key:value,也可以一个
	 *
	 * @param keysValues
	 * @return
	 */
	public   String mset(String... keysValues)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.mset(keysValues);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 批量的设置key:value,可以一个,如果key已经存在则会失败,操作会回滚
	 *
	 * @param keysValues
	 * @return
	 */
	public   Long msetnx(String... keysValues)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.msetnx(keysValues);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 设置key的值,并返回一个旧值
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public   String getSet(String key, String value)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.getSet(key, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过下标 和key 获取指定下标位置的 value
	 *
	 * @param key
	 * @param startOffset
	 * @param endOffset
	 * @return
	 */
	public   String getrange(String key, int startOffset, int endOffset)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.getrange(key, startOffset, endOffset);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key 对value进行加值+1操作,当value不是int类型时会返回错误,当key不存在是则value为1
	 *
	 * @param key
	 * @return
	 */
	public   Long incr(String key)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.incr(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key给指定的value加值,如果key不存在,则这是value为该值
	 *
	 * @param key
	 * @param integer
	 * @return
	 */
	public   Long incrBy(String key, long integer)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.incrBy(key, integer);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 对key的值做减减操作,如果key不存在,则设置key为-1
	 *
	 * @param key
	 * @return
	 */
	public   Long decr(String key)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.decr(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 减去指定的值
	 *
	 * @param key
	 * @param integer
	 * @return
	 */
	public   Long decrBy(String key, long integer)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.decrBy(key, integer);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取value值的长度
	 *
	 * @param key
	 * @return
	 */
	public   Long strLen(String key)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.strlen(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	// TODO-------------------------------------hash--Redis 中每个 hash 可以存储 232 - 1 键值对（40多亿）------------------------------------------------------------

	/**
	 * 通过key给field设置指定的值,如果key不存在则先创建,如果field已经存在,返回0
	 *
	 * @param key
	 * @param field
	 * @param value
	 * @return
	 */
	public   Long hsetnx(String key, String field, String value)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.hsetnx(key, field, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key给field设置指定的值,如果key不存在,则先创建
	 *
	 * @param key
	 * @param field
	 * @param value
	 * @return
	 */
	public   Long hset(String key, String field, String value)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.hset(key, field, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key同时设置 hash的多个field
	 *
	 * @param key
	 * @param hash
	 * @return
	 */
	public   String hmset(String key, Map<String, String> hash)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.hmset(key, hash);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key 和 field 获取指定的 value
	 *
	 * @param key
	 * @param failed
	 * @return
	 */
	public   String hget(String key, String failed)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.hget(key, failed);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}


	/**
	 * 通过key 和 fields 获取指定的value 如果没有对应的value则返回null
	 *
	 * @param key
	 * @param fields
	 *            可以是 一个String 也可以是 String数组
	 * @return
	 */
	public   List<String> hmget(String key, String... fields)
	{
		Jedis jedis = getJedis();
		List<String> result = null;
		try
		{
			result = jedis.hmget(key, fields);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key给指定的field的value加上给定的值
	 *
	 * @param key
	 * @param field
	 * @param value
	 * @return
	 */
	public   Long hincrby(String key, String field, Long value)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.hincrBy(key, field, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key和field判断是否有指定的value存在
	 *
	 * @param key
	 * @param field
	 * @return
	 */
	public   Boolean hexists(String key, String field)
	{
		Jedis jedis = getJedis();
		Boolean result = null;
		try
		{
			result = jedis.hexists(key, field);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回field的数量
	 *
	 * @param key
	 * @return
	 */
	public   Long hlen(String key)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.hlen(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key 删除指定的 field
	 *
	 * @param key
	 * @param fields
	 *            可以是 一个 field 也可以是 一个数组
	 * @return
	 */
	public   Long hdel(String key, String... fields)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.hdel(key, fields);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回所有的field
	 *
	 * @param key
	 * @return
	 */
	public   Set<String> hkeys(String key)
	{
		Jedis jedis = getJedis();
		Set<String> result = null;
		try
		{
			result = jedis.hkeys(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回所有和key有关的value
	 *
	 * @param key
	 * @return
	 */
	public   List<String> hvals(String key)
	{
		Jedis jedis = getJedis();
		List<String> result = null;
		try
		{
			result = jedis.hvals(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取所有的field和value
	 *
	 * @param key
	 * @return
	 */
	public   Map<String, String> hgetall(String key)
	{
		Jedis jedis = getJedis();
		Map<String, String> result = null;
		try
		{
			result = jedis.hgetAll(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	// TODO-------------------------------------------list---一个列表最多可以包含 232 - 1 个元素 (4294967295, 每个列表超过40亿个元素)-----------------------------------------------------------
	/**
	 * 通过key向list头部添加字符串
	 *
	 * @param key
	 * @param strs
	 *            可以是一个string 也可以是string数组
	 * @return 返回list的value个数
	 */
	public   Long lpush(String key, String... strs)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.lpush(key, strs);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key向list尾部添加字符串
	 *
	 * @param key
	 * @param strs
	 *            可以是一个string 也可以是string数组
	 * @return 返回list的value个数
	 */
	public   Long rpush(String key, String... strs)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.rpush(key, strs);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key在list指定的位置之前或者之后 添加字符串元素
	 *
	 * @param key
	 * @param where
	 *            LIST_POSITION枚举类型
	 * @param pivot
	 *            list里面的value
	 * @param value
	 *            添加的value
	 * @return
	 */
	public   Long linsert(String key, BinaryClient.LIST_POSITION where, String pivot, String value)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.linsert(key, where, pivot, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key设置list指定下标位置的value 如果下标超过list里面value的个数则报错
	 *
	 * @param key
	 * @param index
	 *            从0开始
	 * @param value
	 * @return 成功返回OK
	 */
	public   String lset(String key, Long index, String value)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.lset(key, index, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key从对应的list中删除指定的count个 和 value相同的元素
	 *
	 * @param key
	 * @param count
	 *            当count为0时删除全部
	 * @param value
	 * @return 返回被删除的个数
	 */
	public   Long lrem(String key, long count, String value)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.lrem(key, count, value);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key保留list中从strat下标开始到end下标结束的value值
	 *
	 * @param key
	 * @param start
	 * @param end
	 * @return 成功返回OK
	 */
	public   String ltrim(String key, long start, long end)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.ltrim(key, start, end);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key从list的头部删除一个value,并返回该value
	 *
	 * @param key
	 * @return
	 */
	public   synchronized String lpop(String key)
	{

		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.lpop(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key从list尾部删除一个value,并返回该元素
	 *
	 * @param key
	 * @return
	 */
	synchronized public   String rpop(String key)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.rpop(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key从一个list的尾部删除一个value并添加到另一个list的头部,并返回该value 如果第一个list为空或者不存在则返回null
	 *
	 * @param srckey
	 * @param dstkey
	 * @return
	 */
	public   String rpoplpush(String srckey, String dstkey)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.rpoplpush(srckey, dstkey);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取list中指定下标位置的value
	 *
	 * @param key
	 * @param index
	 * @return 如果没有返回null
	 */
	public   String lindex(String key, long index)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.lindex(key, index);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回list的长度
	 *
	 * @param key
	 * @return
	 */
	public   Long llen(String key)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.llen(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取list指定下标位置的value 如果start 为 0 end 为 -1 则返回全部的list中的value
	 *
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public   List<String> lrange(String key, long start, long end)
	{
		Jedis jedis = getJedis();
		List<String> result = null;
		try
		{
			result = jedis.lrange(key, start, end);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key向指定的set中添加value
	 *
	 * @param key
	 * @param members
	 *            可以是一个String 也可以是一个String数组
	 * @return 添加成功的个数
	 */
	public   Long sadd(String key, String... members)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.sadd(key, members);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key删除set中对应的value值
	 *
	 * @param key
	 * @param members
	 *            可以是一个String 也可以是一个String数组
	 * @return 删除的个数
	 */
	public   Long srem(String key, String... members)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.srem(key, members);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key随机删除一个set中的value并返回该值
	 *
	 * @param key
	 * @return
	 */
	public   String spop(String key)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.spop(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取set中的差集 以第一个set为标准
	 *
	 * @param keys
	 *            可以 是一个string 则返回set中所有的value 也可以是string数组
	 * @return
	 */
	public   Set<String> sdiff(String... keys)
	{
		Jedis jedis = getJedis();
		Set<String> result = null;
		try
		{
			result = jedis.sdiff(keys);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取set中的差集并存入到另一个key中 以第一个set为标准
	 *
	 * @param dstkey
	 *            差集存入的key
	 * @param keys
	 *            可以 是一个string 则返回set中所有的value 也可以是string数组
	 * @return
	 */
	public   Long sdiffstore(String dstkey, String... keys)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.sdiffstore(dstkey, keys);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取指定set中的交集
	 *
	 * @param keys
	 *            可以 是一个string 也可以是一个string数组
	 * @return
	 */
	public   Set<String> sinter(String... keys)
	{
		Jedis jedis = getJedis();
		Set<String> result = null;
		try
		{
			result = jedis.sinter(keys);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取指定set中的交集 并将结果存入新的set中
	 *
	 * @param dstkey
	 * @param keys
	 *            可以 是一个string 也可以是一个string数组
	 * @return
	 */
	public   Long sinterstore(String dstkey, String... keys)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.sinterstore(dstkey, keys);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回所有set的并集
	 *
	 * @param keys
	 *            可以 是一个string 也可以是一个string数组
	 * @return
	 */
	public   Set<String> sunion(String... keys)
	{
		Jedis jedis = getJedis();
		Set<String> result = null;
		try
		{
			result = jedis.sunion(keys);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回所有set的并集,并存入到新的set中
	 *
	 * @param dstkey
	 * @param keys
	 *            可以 是一个string 也可以是一个string数组
	 * @return
	 */
	public   Long sunionstore(String dstkey, String... keys)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.sunionstore(dstkey, keys);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key将set中的value移除并添加到第二个set中
	 *
	 * @param srckey
	 *            需要移除的
	 * @param dstkey
	 *            添加的
	 * @param member
	 *            set中的value
	 * @return
	 */
	public   Long smove(String srckey, String dstkey, String member)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.smove(srckey, dstkey, member);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取set中value的个数
	 *
	 * @param key
	 * @return
	 */
	public   Long scard(String key)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.scard(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key判断value是否是set中的元素
	 *
	 * @param key
	 * @param member
	 * @return
	 */
	public   Boolean sismember(String key, String member)
	{
		Jedis jedis = getJedis();
		Boolean result = null;
		try
		{
			result = jedis.sismember(key, member);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取set中随机的value,不删除元素
	 *
	 * @param key
	 * @return
	 */
	public   String srandmember(String key)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.srandmember(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取set中所有的value
	 *
	 * @param key
	 * @return
	 */
	public   Set<String> smembers(String key)
	{
		Jedis jedis = getJedis();
		Set<String> result = null;
		try
		{
			result = jedis.smembers(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key向zset中添加value,score,其中score就是用来排序的 如果该value已经存在则根据score更新元素
	 *
	 * @param key
	 * @param score
	 * @param member
	 * @return
	 */
	public   Long zadd(String key, double score, String member)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.zadd(key, score, member);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key删除在zset中指定的value
	 *
	 * @param key
	 * @param members
	 *            可以 是一个string 也可以是一个string数组
	 * @return
	 */
	public   Long zrem(String key, String... members)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.zrem(key, members);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key增加该zset中value的score的值
	 *
	 * @param key
	 * @param score
	 * @param member
	 * @return
	 */
	public   Double zincrby(String key, double score, String member)
	{
		Jedis jedis = getJedis();
		Double result = null;
		try
		{
			result = jedis.zincrby(key, score, member);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回zset中value的排名 下标从小到大排序
	 *
	 * @param key
	 * @param member
	 * @return
	 */
	public   Long zrank(String key, String member)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.zrank(key, member);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回zset中value的排名 下标从大到小排序
	 *
	 * @param key
	 * @param member
	 * @return
	 */
	public   Long zrevrank(String key, String member)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.zrevrank(key, member);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key将获取score从start到end中zset的value socre从大到小排序 当start为0 end为-1时返回全部
	 *
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public   Set<String> zrevrange(String key, long start, long end)
	{
		Jedis jedis = getJedis();
		Set<String> result = null;
		try
		{
			result = jedis.zrevrange(key, start, end);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回指定score内zset中的value
	 *
	 * @param key
	 * @param max
	 * @param min
	 * @return
	 */
	public   Set<String> zrangebyscore(String key, String max, String min)
	{
		Jedis jedis = getJedis();
		Set<String> result = null;
		try
		{
			result = jedis.zrevrangeByScore(key, max, min);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回指定score内zset中的value
	 *
	 * @param key
	 * @param max
	 * @param min
	 * @return
	 */
	public   Set<String> zrangeByScore(String key, double max, double min)
	{
		Jedis jedis = getJedis();
		Set<String> result = null;
		try
		{
			result = jedis.zrevrangeByScore(key, max, min);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 返回指定区间内zset中value的数量
	 *
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	public   Long zcount(String key, String min, String max)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.zcount(key, min, max);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key返回zset中的value个数
	 *
	 * @param key
	 * @return
	 */
	public   Long zcard(String key)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.zcard(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key获取zset中value的score值
	 *
	 * @param key
	 * @param member
	 * @return
	 */
	public   Double zscore(String key, String member)
	{
		Jedis jedis = getJedis();
		Double result = null;
		try
		{
			result = jedis.zscore(key, member);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key删除给定区间内的元素
	 *
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public   Long zremrangeByRank(String key, long start, long end)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.zremrangeByRank(key, start, end);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key删除指定score内的元素
	 *
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public   Long zremrangeByScore(String key, double start, double end)
	{
		Jedis jedis = getJedis();
		Long result = null;
		try
		{
			result = jedis.zremrangeByScore(key, start, end);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 返回满足pattern表达式的所有key keys(*) 返回所有的key
	 *
	 * @param pattern
	 * @return
	 */
	public   Set<String> keys(String pattern)
	{
		Jedis jedis = getJedis();
		Set<String> result = null;
		try
		{
			result = jedis.keys(pattern);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	/**
	 * 通过key判断值得类型
	 *
	 * @param key
	 * @return
	 */
	public   String type(String key)
	{
		Jedis jedis = getJedis();
		String result = null;
		try
		{
			result = jedis.type(key);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			close(jedis);
		}
		return result;
	}

	private   void close(Jedis jedis)
	{
		if (jedis != null)
		{
			jedis.close();//10500
			//pool.returnBrokenResource(jedis);//55137
			//pool.returnResource(jedis);//10788
			//pool.returnResourceObject(jedis);//11057
			
		}
	}

	public static  Jedis getJedis()
	{
		if (jedis==null)
		{
			jedis=pool.getResource();
		}
		return jedis;
	}

	public static   RedisUtil getRedisUtil()
	{
		return new RedisUtil();
	}

}
