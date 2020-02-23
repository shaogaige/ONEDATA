/**
 * ClassName:CacheManager.java
 * Date:2018年7月9日
 */
package com.idata.core;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Creater:SHAO Gaige
 * Description:token缓存时间控制
 * Log:
 */
public class TokenTimeManager {
	
	private static int MAX = 100;
	
	private static Map<String,Long> cache = new HashMap<String,Long>();
	
	private static SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public static  boolean check(String key)
	{
		Date date=new Date();
		long value = Long.parseLong(df.format(date));
		if(cache.get(key) == null)
		{
			addKey(key,value);
		}
		else
		{
			long v = cache.get(key);
			if(value-v<5*60)
			{
				return false;
			}
			else
			{
				addKey(key,value);
			}
		}
		return true;
	}
	
	private static void addKey(String key,long value)
	{
		int size = cache.size();
		if(size>MAX)
		{
			//剔除不可用数据
			Date date=new Date();
			long values = Long.parseLong(df.format(date));
			
			Iterator<String> iterator_t = cache.keySet().iterator();
			while (iterator_t.hasNext()){
				String keys = iterator_t.next();
				Long v = cache.get(keys);
				Long d = values - v;
				if(d>5*60)
				{
					iterator_t.remove();
				}
			}
		}
		cache.put(key, value);
	}

}
