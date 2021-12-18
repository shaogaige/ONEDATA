/**
 * ClassName:LicenseUtil.java
 * Date:2021年5月17日
 */
package com.idata.data;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.idata.core.DataParam;
import com.idata.core.OneDataServer;
import com.idata.core.SuperObject;
import com.idata.core.SystemManager;
import com.idata.tool.AESUtil;
import com.idata.tool.DateCoder;
import com.idata.tool.FileUtil;
import com.idata.tool.IPAddressUtil;
import com.idata.tool.LogUtil;
import com.idata.tool.PropertiesUtil;


public class LicenseUtil implements IDataDriver{
	//pinlvhaomiao
	private static final long PERIOD = 1000*60*24*10;
	//shouquanjiangemiao
	private static final int DISTANCE = 60*60*24*90;
	
	private static String KEYS = "ONEDATASERVER";
	
	private static long count = 0;

	@Override
	public boolean add(DataParam param) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean edit(DataParam param) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete(DataParam param) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<SuperObject> query(DataParam param) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<SuperObject> group(DataParam param) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<SuperObject> getMeta(DataParam param) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isSupport() {
		// TODO Auto-generated method stub
		if(count<1)
		{
			produce();
		}
		return false;
	}
	
	public class Task extends TimerTask
	{
		@Override
		public void run() {
			// TODO Auto-generated method stub
			count++;
			String key = AESUtil.aesDecrypt(KEYS, OneDataServer.AESKEY);
			DateCoder coder = new DateCoder();
			//System.out.println(coder.uncode(key.substring(0,14)));
			//System.out.println(new Date());
			if(addSeconds(coder.uncode2Date(key.substring(0,14)),DISTANCE).before(new Date()))
			{
				SystemManager.setSystemHandle(null);
				LogUtil.info("\u670d\u52a1\u6388\u6743\u7801\u5df2\u7ecf\u5230\u671f\uff01\u8bf7\u8054\u7cfb\u7ba1\u7406\u5458\uff01\uff01\uff01");
			}
			else
			{
				// request server
				if(count<2)
				{
					System.out.println("\u670d\u52a1\u6388\u6743\u7801\u6b63\u5e38\u002e\u002e\u002e");
				}
				else
				{
					LogUtil.info("\u670d\u52a1\u6388\u6743\u7801\u6b63\u5e38\u002e\u002e\u002e");
				}
				
			}
		}
	}
	
	public void check() {
		Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 1);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date date=calendar.getTime();
        if (date.before(new Date())) {
            date = this.addSeconds(date, 60);
        }
        Timer timer = new Timer();
        Task task = new Task();
        timer.schedule(task,date,PERIOD);
	}
	public Date addSeconds(Date date, int num) {  
        Calendar startDT = Calendar.getInstance();  
        startDT.setTime(date);  
        startDT.add(Calendar.SECOND, num);  
        return startDT.getTime();
    }  
	
	public void produce() 
	{
		String path = PropertiesUtil.getValue("_systempath_");
		DateCoder coder = new DateCoder();
		String key = coder.code(null);
		//System.out.println(coder.uncode(key));
		key += IPAddressUtil.getIPAddress();
		key = AESUtil.aesEncrypt(key, OneDataServer.AESKEY);
		
		if(PropertiesUtil.existFile(path, ".lic"))
		{
			KEYS = PropertiesUtil.getValue("lic");
		}
		else
		{
			path += "//"+"auth.lic";
			path = path.replaceAll("//", "/").replaceAll("\\\\", "/");
			KEYS = key;
			key = "lic="+key;
			FileUtil.writeFile(path, key.getBytes());
			System.out.println("\u6b63\u5728\u751f\u6210\u6388\u6743\u6587\u4ef6\u002e\u002e\u002e");
		}
		check();
	}

	@Override
	public int getClassOrder() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isSupport(String fun) {
		// TODO Auto-generated method stub
		switch(fun){
		  case "add" :
			  return false;
		  case "adds" :
			  return false;
	      case "edit" :
	    	  return false; 
	      case "delete" :
	    	  return false; 
	      case "query" :
	    	  return false; 
	      case "group" :
	    	  return false; 
	      case "getmeta" :
	    	  return false; 
	      default : 
	          return false;
	    }
	}

	@Override
	public boolean adds(DataParam param) {
		// TODO Auto-generated method stub
		return false;
	}
	

}
