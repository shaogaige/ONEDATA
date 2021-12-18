/**
 * ClassName:FileUtil.java
 * Date:2017年2月24日
 */
package com.idata.tool;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

/**
 * Creater: ShaoGaige
 * Description:文件读取写入类
 * Log:
 */
public class FileUtil {
	
	/**
	 * 读取文件内容为字符串
	 * @param fileName
	 * @return String
	 */
	public static String readFileContent(String filePath) {
	    File file = new File(filePath);
	    if(!file.exists())
	    {
	    	return null;
	    }
	    BufferedReader reader = null;
	    StringBuffer sbf = new StringBuffer();
	    try {
	        reader = new BufferedReader(new FileReader(file));
	        String tempStr;
	        while ((tempStr = reader.readLine()) != null) {
	            sbf.append(tempStr);
	        }
	        reader.close();
	        return sbf.toString();
	    } catch (IOException e) {
	        e.printStackTrace();
	        return null;
	    } finally {
	        if (reader != null) {
	            try {
	                reader.close();
	            } catch (IOException e1) {
	                e1.printStackTrace();
	            }
	        }
	    }
	    
	}
	
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
	
	public static void writeFile(String path,InputStream in)
	{
		
	    try 
	    {
	    	File file = new File(path);
	    	File dir = new File(file.getParent());
			if (!dir.exists()) {
				dir.mkdirs();
			}
			FileOutputStream fos = new FileOutputStream(path);
			byte[] b = new byte[1024*1024];
			while ((in.read(b)) != -1) {
			    fos.write(b);// 写入数据
			}
			fos.close();// 保存数据
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
	
	/**
	 * 文件是否存在
	 * @param path
	 * @return
	 */
	public static boolean existsFile(String path)
	{
		File file = new File(path);
		return file.exists();
	}
	
	/**
	 * 获取文件名或后缀的的文件路径
	 * @param path
	 * @param filename
	 * @return
	 */
	public static boolean getFilePath(String path,String filename,String[] outpath)
	{
		File file = new File(path);
		if(file.isFile())
		{
			if(file.getAbsolutePath().endsWith(filename))
			{
				outpath[0] = file.getAbsolutePath();
				return true;
			}
		}
		else
		{
			File[] files = file.listFiles();
			for (File f : files)
			{
				if(getFilePath(f.getAbsolutePath(),filename,outpath))
				{
					break;
				}
			}
		}
		return false;
		
		
	}

}
