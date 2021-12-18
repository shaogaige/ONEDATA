/**
 * ClassName:RandomIDUtil.java
 * Date:2018年9月20日
 */
package com.idata.tool;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.google.gson.Gson;

/**
 * Creater:SHAO Gaige
 * Description:
 * Log:
 */
public class RandomIDUtil {
	
	public static String web_randomorder(String params)
	{
		Gson gson = new Gson();
		@SuppressWarnings("unchecked")
		HashMap<String,List<Double>> ps = gson.fromJson(params,new HashMap<String,ArrayList<Double>>().getClass());
		List<Double> p1 = ps.get("param1");
		Integer[] arr = new Integer[p1.size()];
		for(int i=0;i<p1.size();i++)
		{
			arr[i] = p1.get(i).intValue();
		}
		arr = getRandomOrder(arr);
		return gson.toJson(arr);
	}
	
	public static String web_random(String params)
	{
		Gson gson = new Gson();
		@SuppressWarnings("unchecked")
		HashMap<String,Double> ps = gson.fromJson(params,new HashMap<String,Double>().getClass());
		Double p1 = ps.get("param1");
		String result = getRandom("").substring(0, p1.intValue());
		return result;
	}
	
	public static String web_uuid(String params)
	{
		return getUUID("");
	}
	
	public static<T> T[] getRandomOrder(T[] arr)
	{
		if(arr == null)
		{
			return null;
		}
		int length = arr.length;
		Random rand = new Random();
		for(int i=length;i>0;i--)
		{
			int rid = rand.nextInt(i);
			
			T temp = arr[rid];
			arr[rid] = arr[i-1];
			arr[i-1] = temp;
		}
		return arr;
	}
	
	
	public static String getDate(String prefix)
	{
		String ID = prefix;
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date=new Date();
		ID += df.format(date);
		return ID;
		  
	}
	
	public static String getRandom(String prefix)
	{
		String ID = prefix;
		String[] chars = new String[] { "a", "b", "c", "d", "e", "f",
							"g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s",
							"t", "u", "v", "w", "x", "y", "z", "0", "1", "2", "3", "4", "5",
							"6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I",
							"J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
							"W", "X", "Y", "Z" };
		StringBuffer shortBuffer = new StringBuffer();
		String uuid = UUID.randomUUID().toString().replace("-", "");
		for (int i = 0; i< 8; i++) {
		    String str = uuid.substring(i * 4, i * 4 + 4);
			int x = Integer.parseInt(str, 16);
			shortBuffer.append(chars[x % 0x3E]);
		}
		//System.out.println(shortBuffer.toString());
		ID += shortBuffer.toString();
		return ID;
		  
	}
	
	public static long getNumberID()
	{
		Date date=new Date();
		return date.getTime();
	}
	
	public static String getUUID(String prefix)
	{
		String ID = prefix;
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date=new Date();
		ID += df.format(date);
		ID += getRandom("_");
		return ID;
		  
	}

}
