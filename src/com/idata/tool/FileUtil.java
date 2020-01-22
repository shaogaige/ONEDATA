/**
 * ClassName:FileUtil.java
 * Date:2017年2月24日
 */
package com.idata.tool;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Creater: ShaoGaige
 * Description:文件读取写入类
 * Log:
 */
public class FileUtil {
	
	/**
	 * 根据路径读取文件成byte数组
	 * @param path
	 * @return byte[]
	 */
	public static byte[] readFile(String path)
	{
		try 
		{
			FileInputStream file = new FileInputStream(path);
			BufferedInputStream in = new BufferedInputStream(file);
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024*1024);
			byte[] temp = new byte[1024*1024];     
	        int size = 0;     
	        while ((size = in.read(temp)) != -1) 
	        {     
	            out.write(temp, 0, size);     
	        }     
	        in.close();     
	        byte[] content = out.toByteArray();
	        
	        return content;
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 写入文件
	 * @param path
	 * @param content
	 */
	public static void writeFile(String path,byte[] content)
	{
		
	    try 
	    {
	    	
	    	File file = new File(path);
	    	File dir = new File(file.getParent());
			if (!dir.exists()) {
				dir.mkdirs();
			}
	    	FileOutputStream fos = new FileOutputStream(path);
			fos.write(content);
			fos.close(); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 删除文件
	 * @param file
	 * @return
	 */
	public static boolean delFile(File file)
	{
		if(file == null)
		{
			return false;
		}
		//File file = new File(path);
		if (!file.exists()) {
		    return false;
		}
		 
		if (file.isDirectory()) {
		    File[] files = file.listFiles();
		    for (File f : files) {
		        delFile(f);
		    }
		}
		return file.delete();
    }

}
