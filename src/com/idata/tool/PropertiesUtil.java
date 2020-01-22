package com.idata.tool;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Properties;

import org.wltea.analyzer.core.IKSegmenter;

public class PropertiesUtil {
	
	private static Properties systemProperties = new Properties();
	
	/**
	 * 初始化配置文件
	 */
	public static void init() 
{
	    try 
		{
	    	//读取系统配置文件
	    	InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream("config/config.properties");
	    	systemProperties.load(in);
	    	//System.out.println("已加载系统配置文件...");
	    	//读取索引配置文件
	    	String indexPath = getValue("SEARCH_INDEX_PATH");
	    	//检测分词词典
			if(existFile(indexPath,".dic"))
			{
				System.out.println("检测到有用户自定义词典...");
				String epath = indexPath+"//"+"ext_user.dic";
				epath = epath.replaceAll("//", "/");
				
				//加载分词词典
				IKSegmenter ikSeg = new IKSegmenter(new StringReader("河南数慧信息技术有限公司"),true,epath);
				System.out.println(ikSeg.toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			systemProperties = null;
			System.out.println("读取配置文件信息失败！！！");
		}
	}
	
	/**
	 * 是否存在对应文件
	 * @param path
	 * @param suffix
	 * @return boolean
	 */
	public static boolean existFile(String path,String suffix)
	{
		File file = new File(path);
		if(file.isDirectory())
		{
			File[] files = file.listFiles();
			for(File f:files)
			{
				if(f.getAbsolutePath().endsWith(suffix))
				{
					return true;
				}
			}
		}
		else
		{
			if(file.getAbsolutePath().endsWith(suffix))
			{
				return true;
			}
		}
	    return false;
	}	
	/**
	 * 获取对应KEY的值
	 * @param key
	 * @return String
	 */
	public static String getValue(String key)
	{
		if(systemProperties != null) 
		{
			String value = systemProperties.getProperty(key, null);
			return value;
		}
		else
		{
			return null;
		}
	}
	
}
