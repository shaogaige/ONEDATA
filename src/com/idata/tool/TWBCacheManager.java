/**
 * ClassName:TWBCacheManager.java
 * Date:2020年2月15日
 */
package com.idata.tool;

import java.util.Date;
import java.util.Deque;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;


/**
 * Creater:SHAO Gaige
 * Description:三路均衡缓存管理器
 * Log:三路均衡缓存淘汰算法已申请专利
 * 
 */
public class TWBCacheManager<T> {
	
	//最近问列表长度
	private int RecentAccessList_size = 100;
	//当前热点集合长度
	private int CurrentHotSet_size = 20;
	//历史热点队列长度
	private int PastHotQueue_size = 50;
	//最近问列表
	private Deque<String> recent = new LinkedBlockingDeque<String>();
	//当前热点集合
	private Vector<String> current = new Vector<String>();
	//历史热点优先级队列
	private Queue<Value> past = new PriorityBlockingQueue<Value>();
	
	//缓存存储
	private Map<String,Value> cache = new ConcurrentHashMap<String,Value>();
	
	public TWBCacheManager()
	{
		
	}
	
	public TWBCacheManager(int recent,int current,int past)
	{
		this.RecentAccessList_size = recent;
		this.CurrentHotSet_size = current;
		this.PastHotQueue_size = past;
	}
	
	@SuppressWarnings("unchecked")
	public T getObject(String key)
	{
		Value v = this.get(key);
		if(v != null)
		{
			return (T)v.getValue_o();
		}
		else
		{
			return null;
		}
	}
	
	public Value get(String key)
	{
		if(this.cache.containsKey(key))
		{
			Value v = this.cache.get(key);
			v.setCounts(v.getCounts()+1);
			this.cache.put(key, v);
			if(recent.contains(key))
			{
				//最近访问列表
				if(!key.equalsIgnoreCase(recent.peekFirst()))
				{
					//放到队列头部
					recent.remove(key);
					recent.addFirst(key);
				}
				else
				{
					//调整到当前热点集合
					int csize = current.size();
					if(csize<CurrentHotSet_size)
					{
						//直接加入到当前热点集合
						recent.remove(key);
						current.add(key);
					}
					else
					{
						long now = getTime();
						String min_k = current.elementAt(0);
						double f = cache.get(min_k).getFrequency(now);
						//计算访问频率最小的
						for(int i=1;i<csize;i++)
						{
							String k = current.elementAt(i);
							Value cv = cache.get(k);
							double t = cv.getFrequency(now);
							if(t<f)
							{
								f = t;
								min_k = k;
							}
						}
						
						recent.remove(key);
						current.remove(min_k);
						current.add(key);
						
						//判断历史热点队列
						past.add(cache.get(min_k));
						int psize = past.size();
						if(psize>PastHotQueue_size)
						{
							Value mv = past.poll();
							cache.remove(mv.getKey());
						}
					}
				}
			}
			else if(!current.contains(key))
			{
				//在历史热点队列
				past.remove(v);
				recent.addFirst(key);
				int rsize = recent.size();
				if(rsize>RecentAccessList_size)
				{
					String k = recent.removeLast();
					cache.remove(k);
				}
				
				
//				int csize = current.size();
//				if(csize<CurrentHotSet_size)
//				{
//					//直接加入到当前热点集合
//					past.remove(v);
//					current.add(key);
//				}
//				else
//				{
//					long now = getTime();
//					String min_k = current.elementAt(0);
//					double f = cache.get(min_k).getFrequency(now);
//					//计算访问频率最小的
//					for(int i=1;i<csize;i++)
//					{
//						String k = current.elementAt(i);
//						Value cv = cache.get(k);
//						double t = cv.getFrequency(now);
//						if(t<f)
//						{
//							f = t;
//							min_k = k;
//						}
//					}
//					
//					current.remove(min_k);
//					current.add(key);
//					
//					//判断历史热点队列
//					past.add(cache.get(min_k));
//					int psize = past.size();
//					if(psize>PastHotQueue_size)
//					{
//						Value mv = past.poll();
//						cache.remove(mv.getKey());
//					}
//				}
				
				
			}
			
			return v;
		}
		else
		{
			return null;
		}
	}
	
	public boolean add(String key,T t)
	{
		Value v = new Value();
		if(cache.containsKey(key))
		{
			//exists
			Value old = cache.get(key);
			v.setTime(old.getTime());
			v.setCounts(old.getCounts());
			v.setKey(key);
			v.setValue_o(t);
			cache.put(key, v);
		}
		else
		{
			//new object
			v.setTime(getTime());
			v.setKey(key);
			v.setValue_o(t);
			recent.addFirst(key);
			cache.put(key, v);
			int rsize = recent.size();
			if(rsize>RecentAccessList_size)
			{
				String k = recent.removeLast();
				cache.remove(k);
			}
		}
		return true;
		
	}
	
	public boolean removeall()
	{
		this.cache.clear();
		this.recent.clear();
		this.current.clear();
		this.past.clear();
		return true;
	}
	
	public boolean remove(String key)
	{
		if(cache.containsKey(key))
		{
			Value v = cache.get(key);
			if(current.contains(key))
			{
				current.remove(key);
			}
			else if(recent.contains(key))
			{
				recent.remove(key);
			}
			else
			{
				past.remove(v);
			}
			cache.remove(key);
			return true;
		}
		return false;
	}
	
	public void removeContainKey(String key)
	{
		for(String _key:this.cache.keySet()) 
		{
			if(_key.equalsIgnoreCase(key) || _key.contains(key) || key.contains(_key)) 
			{
				//System.out.println("remove:"+_key);
				this.remove(_key);
			}
		}
	}
	
	public boolean containsKey(String key)
	{
		return this.cache.containsKey(key);
	}
	
	public void print()
	{
		System.out.println("size:"+cache.size()+";rsize:"+recent.size()+";csize:"+current.size()+";psize:"+past.size());
		//System.out.println(current.get(0)+current.get(1));
		//System.out.println(past.peek().getKey());
		System.out.println("key:"+this.cache.keySet().toString());
	}
	
	private long getTime()
	{
		Date now = new Date();
		return now.getTime();
	}
	
	
	/**
	 * 
	 * Creater:SHAO Gaige
	 * Description:存储对象内部静态类
	 * Log:
	 */
	public static class Value implements Comparable<Value>
	{
		//key
		private String key;
		//类型
		private int type = 1;
		//1-字符串
		private String value_s;
		//2-字节数组
		private byte[] value_bs;
		//3-对象
		private Object value_o;
		//时间
		private long time;
		//次数
		private long counts = 0;
		
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			this.key = key;
		}
		public double getFrequency(long now)
		{
			return (now-this.time)*1.0/this.counts;
		}
		public long getTime() {
			return time;
		}
		public void setTime(long time) {
			this.time = time;
		}
		public long getCounts() {
			return counts;
		}
		public void setCounts(long counts) {
			this.counts = counts;
		}
		public int getType() {
			return type;
		}
		public void setType(int type) {
			this.type = type;
		}
		public String getValue_s() {
			return value_s;
		}
		public void setValue_s(String value_s) {
			this.value_s = value_s;
			this.type = 1;
		}
		public byte[] getValue_bs() {
			return value_bs;
		}
		public void setValue_bs(byte[] value_bs) {
			this.value_bs = value_bs;
			this.type = 2;
		}
		public Object getValue_o() {
			return value_o;
		}
		public void setValue_o(Object value_o) {
			this.value_o = value_o;
		}
		@Override
		public int compareTo(Value o) {
			// TODO Auto-generated method stub
			if(this.counts<o.counts) {
				return -1;
			}else if(this.counts>o.counts) {
				return 1;
			}else {
				return 0;
			}
		}
	}

}
