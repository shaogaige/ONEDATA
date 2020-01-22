/**
 * ClassName:GZIPUtil.java
 * Date:2017年2月12日
 */
package com.idata.tool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Creater: ShaoGaige
 * Description:GZIP压缩解压类
 * Log:
 */
public class GZIPUtil {
	
	/**
	 * GZIP压缩
	 * @param data
	 * @return byte[]
	 */
	public static byte[] gZip(byte[] data) 
	{
		byte[] b = null;
		try 
		{
		   ByteArrayOutputStream bos = new ByteArrayOutputStream();
		   GZIPOutputStream gzip = new GZIPOutputStream(bos);
		   gzip.write(data);
		   gzip.finish();
		   gzip.close();
		   b = bos.toByteArray();
		   bos.close();
		}
		catch (Exception ex) 
		{
		   ex.printStackTrace();
		}
		return b;
	}
	
	/**
	 * GZIP解压
	 * @param data
	 * @return byte[]
	 */
	public static byte[] unGZip(byte[] data) 
	{
		byte[] b = null;
		try 
		{
		   ByteArrayInputStream bis = new ByteArrayInputStream(data);
		   GZIPInputStream gzip = new GZIPInputStream(bis);
		   byte[] buf = new byte[1024*10];
		   int num = -1;
		   ByteArrayOutputStream baos = new ByteArrayOutputStream();
		   while ((num = gzip.read(buf, 0, buf.length)) != -1) {
		    baos.write(buf, 0, num);
		   }
		   b = baos.toByteArray();
		   baos.flush();
		   baos.close();
		   gzip.close();
		   bis.close();
		} 
		catch (Exception ex) 
		{
		   ex.printStackTrace();
		}
		return b;
	}
		  

}
