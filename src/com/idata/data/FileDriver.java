/**
 * ClassName:FileDriver.java 
 * Date:2021年6月23日
 */
package com.idata.data;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;
import com.idata.core.DataParam;
import com.idata.core.OneDataServer;
import com.idata.core.SuperObject;
import com.idata.tool.FileUtil;
import com.idata.tool.PropertiesUtil;
import com.idata.tool.RandomIDUtil;
import com.ojdbc.sql.Value;

/**
 * Creater:SHAO Gaige
 * Description:文件类型驱动类
 * Log:
 */
public class FileDriver implements IDataDriver {

	@Override
	public boolean add(DataParam param) {
		// TODO Auto-generated method stub
		if(param.getFiles() == null || param.getFiles().size()<1)
		{
			return true;
		}
		String fileNames = PropertiesUtil.getValue("FILE_NAME_FIELDS");
		String fileUrls = PropertiesUtil.getValue("FILE_URL_FIELDS");
		String filePath = PropertiesUtil.getValue("FILE_PATH");
		String fileServer = PropertiesUtil.getValue("FILE_SERVER");
		if(fileNames.contains(","))
		{
			//多个文件字段,多对多
			String[] names = fileNames.split(",");
			String[] urls = fileUrls.split(",");
			int size = names.length;
			for(int i=0;i<size;i++)
			{
				String fieldname = names[i];
				String fileName = param.getJsonDataValue(fieldname).getString_value();
				if("".equalsIgnoreCase(fileName) || !fileName.contains("."))
				{
					continue;
				}
				byte[] content = param.getFiles().get(fileName);
				if(content == null)
				{
					continue;
				}
				//存储
				String path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
				if(FileUtil.existsFile(path))
				{
					String pre = fileName.substring(0,fileName.lastIndexOf("."));
					String uuid = pre + RandomIDUtil.getUUID("_");
					fileName = fileName.replace(pre, uuid);
					path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
				}
				FileUtil.writeFile(path, content);
				param.addPath(path);
				//生成连接
				String fielurl = fileServer+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
				String urlfield = urls[i];
				param.setJsonDataProperty(urlfield, new Value().setString_value(fielurl));
			}
		}
		else
		{
			//一个文件字段,一对一或一对多
			String fileUrlsValue = "";
			Map<String, byte[]> files = param.getFiles();
			for(String key:files.keySet())
			{
				byte[] content = files.get(key);
				//存储
				String path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+key;
				if(FileUtil.existsFile(path))
				{
					String pre = key.substring(0,key.lastIndexOf("."));
					String uuid = pre + RandomIDUtil.getUUID("_");
					key = key.replace(pre, uuid);
					path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+key;
				}
				FileUtil.writeFile(path, content);
				param.addPath(path);
				//生成连接
				String fileurl = fileServer+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+key;
				fileUrlsValue += fileurl + ",";
			}
			if(fileUrlsValue.endsWith(","))
			{
				fileUrlsValue = fileUrlsValue.substring(0, fileUrlsValue.length()-1);
			}
			param.setJsonDataProperty(fileUrls, new Value().setString_value(fileUrlsValue));
		}
		return true;
	}
	
	/**
	 * 文件上传接口
	 * @param files
	 * @return String
	 */
	public JsonObject add(String key,byte[] file)
	{
		String filePath = PropertiesUtil.getValue("FILE_PATH");
		String fileServer = PropertiesUtil.getValue("FILE_SERVER");
		JsonObject fo = new JsonObject();
		String fileName = key;
		if("".equalsIgnoreCase(fileName))
		{
			return null;
		}
		//byte[] content =files.get(fileName);
		//存储
		String path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
		if(FileUtil.existsFile(path))
		{
			String pre = fileName.substring(0,fileName.lastIndexOf("."));
			String uuid = pre + RandomIDUtil.getUUID("_");
			fileName = fileName.replace(pre, uuid);
			path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
		}
		FileUtil.writeFile(path, file);
		//生成连接
		String fielurl = fileServer+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
		fo.addProperty(key, fielurl);
		return fo;
	}
	
	public JsonObject add(String key, InputStream in)
	{
		String filePath = PropertiesUtil.getValue("FILE_PATH");
		String fileServer = PropertiesUtil.getValue("FILE_SERVER");
		JsonObject fo = new JsonObject();
		String fileName = key;
		if("".equalsIgnoreCase(fileName))
		{
			return null;
		}
		//byte[] content =files.get(fileName);
		//存储
		String path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
		if(FileUtil.existsFile(path))
		{
			String pre = fileName.substring(0,fileName.lastIndexOf("."));
			String uuid = pre + RandomIDUtil.getUUID("_");
			fileName = fileName.replace(pre, uuid);
			path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
		}
		FileUtil.writeFile(path, in);
		//生成连接
		String fielurl = fileServer+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
		fo.addProperty(key, fielurl);
		return fo;
	}
	
	public boolean add(DataParam param,InputStream in) {
		// TODO Auto-generated method stub
		String fileNames = PropertiesUtil.getValue("FILE_NAME_FIELDS");
		String fileUrls = PropertiesUtil.getValue("FILE_URL_FIELDS");
		String filePath = PropertiesUtil.getValue("FILE_PATH");
		String fileServer = PropertiesUtil.getValue("FILE_SERVER");
		if(fileNames.contains(","))
		{
			//多个文件字段,多对多
			String[] names = fileNames.split(",");
			String[] urls = fileUrls.split(",");
			int size = names.length;
			for(int i=0;i<size;i++)
			{
				String fieldname = names[i];
				String fileName = param.getJsonDataValue(fieldname).getString_value();
				if("".equalsIgnoreCase(fileName) || !fileName.contains("."))
				{
					continue;
				}
//				byte[] content = param.getFiles().get(fileName);
//				if(content == null)
//				{
//					continue;
//				}
				//存储
				String path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
				if(FileUtil.existsFile(path))
				{
					String pre = fileName.substring(0,fileName.lastIndexOf("."));
					String uuid = pre + RandomIDUtil.getUUID("_");
					fileName = fileName.replace(pre, uuid);
					path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
				}
				FileUtil.writeFile(path, in);
				param.addPath(path);
				//生成连接
				String fielurl = fileServer+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
				String urlfield = urls[i];
				param.setJsonDataProperty(urlfield, new Value().setString_value(fielurl));
			}
		}
		else
		{
			//一个文件字段,一对一或一对多
			String fileName = param.getJsonDataValue(fileNames).getString_value();
			String fileUrlsValue = "";
			if(param.getJsonDataValue(fileUrls) != null && !"".equalsIgnoreCase(param.getJsonDataValue(fileUrls).getString_value()))
			{
				fileUrlsValue = param.getJsonDataValue(fileUrls).getString_value();
			}
			//byte[] content = files.get(key);
			//存储
			String path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
			if(FileUtil.existsFile(path))
			{
				String pre = fileName.substring(0,fileName.lastIndexOf("."));
				String uuid = pre + RandomIDUtil.getUUID("_");
				fileName = fileName.replace(pre, uuid);
				path = filePath+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
			}
			FileUtil.writeFile(path, in);
			param.addPath(path);
			//生成连接
			String fileurl = fileServer+"/"+OneDataServer.getYear()+"/"+OneDataServer.getMonth()+"/"+fileName;
			if(!"".equalsIgnoreCase(fileUrlsValue))
			{
				fileUrlsValue += "," + fileurl;
			}
			else
			{
				fileUrlsValue = fileurl;
			}
			
			param.setJsonDataProperty(fileUrls, new Value().setString_value(fileUrlsValue));
		}
		return true;
	}

	@Override
	public boolean edit(DataParam param) {
		// TODO Auto-generated method stub
		add(param);
		return true;
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
		return true;
	}

	@Override
	public int getClassOrder() {
		// TODO Auto-generated method stub
		return 1;
	}

	@Override
	public boolean isSupport(String fun) {
		// TODO Auto-generated method stub
		switch(fun){
		  case "add" :
			  return true;
		  case "adds" :
			  return false;
	      case "edit" :
	    	  return true; 
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
