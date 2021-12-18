package com.idata.tool;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Properties;

import org.gdal.gdal.gdal;
import org.wltea.analyzer.core.IKSegmenter;

import com.idata.core.TableIndexWriter;

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
	    	
	    	String path = PropertiesUtil.class.getClassLoader().getResource("config/config.properties").getPath();
	    	File f = new File(path);
	    	String indexPath = f.getParent();
	    	systemProperties.put("_systempath_", indexPath);
	    	//检测分词词典
			if(existFile(indexPath,".dic"))
			{
				System.out.println("检测到有用户自定义词典...");
				String epath = indexPath+"//"+"ext_user.dic";
				epath = epath.replaceAll("//", "/");
				epath = epath.replaceAll("\\\\", "/");
				
				//加载分词词典
				TableIndexWriter.ikSeg = new IKSegmenter(new StringReader("onedata"),true,epath);
				//System.out.println(epath);
			}
			if(existFile(indexPath,".lic"))
			{
				System.out.println("授权文件检测正常...");
				InputStream is = PropertiesUtil.class.getClassLoader().getResourceAsStream("config/auth.lic");
				Properties lic = new Properties();
		    	lic.load(is);
		    	systemProperties.put("lic", lic.getProperty("lic"));
			}
			//GDAL加载
			String gdal_load = systemProperties.getProperty("GDAL_LOAD");
			if("true".equalsIgnoreCase(gdal_load))
			{
				File f2 = new File(indexPath);
				String mainpath = f2.getParent().replaceAll("//", "/").replaceAll("\\\\", "/");
				String resource = mainpath + "/resource";
				if(existFile(resource,"gdal"))
				{
					System.out.println("检测到GDAL目录并允许加载...");
					try 
					{
						System.setProperty("GDAL_HOME", mainpath+"/resource/gdal/win64/bin");
						System.setProperty("GDAL_HOME_JAVA", mainpath+"/resource/gdal/win64/bin/gdal/java");
						//System.out.println( System.getProperty("java.library.path"));
						System.setProperty("java.library.path", System.getProperty("java.library.path")+";"+mainpath+"/resource/gdal/win64/bin/");
						System.setProperty("java.library.path", System.getProperty("java.library.path")+";"+mainpath+"/resource/gdal/win64/bin/gdal/java/");
						//System.out.println( System.getProperty("java.library.path"));
						LibraryUtil.loadFromResource(systemProperties.getProperty("GDAL_PATH"));
						gdal.AllRegister();
					} 
					catch (IOException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LogUtil.error(e);
			systemProperties = null;
			LogUtil.info("读取配置文件信息失败！！！");
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
