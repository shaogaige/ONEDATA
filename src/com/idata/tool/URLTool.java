/**
 * ClassName:URLTool.java
 * Date:2019年8月26日
 */
package com.idata.tool;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * Creater:SHAO Gaige
 * Description:URL字符串编码解码工具
 * Log:
 */
public class URLTool {
	
	/**
	 * URL编码
	 * @param str
	 * @return
	 */
	public static String encode(String str)
	{
		try 
		{
			return URLEncoder.encode(str, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return str;
		}
	}
	
	/**
	 * URL解码
	 * @param str
	 * @return
	 */
	public static String decode(String str)
	{
		try 
		{
			return URLDecoder.decode(str, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return str;
		}
	}

}
