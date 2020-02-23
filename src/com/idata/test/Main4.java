package com.idata.test;

import com.idata.tool.TWBCacheManager;
import com.idata.tool.TWBCacheManager.Value;

public class Main4 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.out.println("GEOMETRY_IN".equalsIgnoreCase("geometry_in"));
		
		for(int i=0;i<200;i++)
		{
			Value v = new Value();
			v.setValue_s("v"+i);
			TWBCacheManager.add("key"+i, v);
			
			TWBCacheManager.get("key"+0);
			TWBCacheManager.get("key"+1);
			TWBCacheManager.get("key"+2);
			
			if(i%5==0)
			{
				//TWBCacheManager.get("key"+1);
				
				
			}
		}
		
		TWBCacheManager.get("key"+199);
		TWBCacheManager.get("key"+199);
		TWBCacheManager.get("key"+199);
		
		for(int i=0;i<10;i++)
		{
			TWBCacheManager.get("key"+1);
		}
		
		TWBCacheManager.print();

	}

}
