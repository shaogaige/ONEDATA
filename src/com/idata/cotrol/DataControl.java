/**
 * ClassName:DataControl.java
 * Date:2020年1月28日
 */
package com.idata.cotrol;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.idata.apps.IApplication;
import com.idata.core.DataParam;
import com.idata.core.GodTool;
import com.idata.core.OneDataServer;
import com.idata.core.ResultBuilder;
import com.idata.core.SuperObject;
import com.idata.data.DataBaseDriver;
import com.idata.data.HbaseDriver;
import com.idata.data.IDataDriver;
import com.idata.data.IndexDriver;
import com.idata.tool.ClassUtil;
import com.idata.tool.HttpRequestUtil;
import com.idata.tool.LogUtil;
import com.idata.tool.PropertiesUtil;

/**
 * Creater:SHAO Gaige
 * Description:数据访问管理器
 * Log:
 */
public class DataControl {
	
	public String process(DataParam param)
	{
		IDataDriver driver = null;
		String result = "";
		if("query".equalsIgnoreCase(param.getOperation()))
		{
			if("table".equalsIgnoreCase(param.getType()))
			{
				driver = new DataBaseDriver();
			}
			else if("index".equalsIgnoreCase(param.getType()))
			{
				if(!checkIndexParam(param))
				{
					return requestMain(param);
				}
				driver = new IndexDriver();
			}
			else if("hbase".equalsIgnoreCase(param.getType()))
			{
				driver = new HbaseDriver();
			}
			List<SuperObject> os = driver.query(param);
			if(os != null)
			{
				String r = ResultBuilder.object2string(os,param.getOut());
				Long size = IDataDriver.resultSize.getObject(param.toString());
				result = "{\"state\":true,\"message\":\"数据查询成功\",\"size\":"+size+",\"data\":"+r+"}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"数据查询失败\",\"size\":0,\"data\":[]}";
			}
		}
		else if("getmeta".equalsIgnoreCase(param.getOperation()))
		{
			driver = new DataBaseDriver();
			List<SuperObject> rsm = driver.getMeta(param);
			if(rsm != null)
			{
				String r = ResultBuilder.object2string(rsm,"json");
				int size = rsm.size();
				result = "{\"state\":true,\"message\":\"获取元数据成功\",\"size\":"+size+",\"data\":"+r+"}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"获取元数据失败\",\"size\":0,\"data\":[]}";
			}
			
			
		}
		else if("group".equalsIgnoreCase(param.getOperation()))
		{
			if("table".equalsIgnoreCase(param.getType()))
			{
				driver = new DataBaseDriver();
			}
			else if("index".equalsIgnoreCase(param.getType()))
			{
				if(!checkIndexParam(param))
				{
					return requestMain(param);
				}
				driver = new IndexDriver();
			}
			else if("hbase".equalsIgnoreCase(param.getType()))
			{
				driver = new HbaseDriver();
			}
			List<SuperObject> os = driver.group(param);
			if(os != null)
			{
				String r = ResultBuilder.object2string(os,"json");
				int size = os.size();
				result = "{\"state\":true,\"message\":\"数据统计成功\",\"size\":"+size+",\"data\":"+r+"}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"数据统计失败\",\"size\":0,\"data\":[]}";
			}
			
		}
		else if("add".equalsIgnoreCase(param.getOperation()))
		{
			boolean flag = true;
			boolean once = true;
			List<Class<?>> subClass = ClassUtil.getClasses("com.idata.data", IDataDriver.class);
			classOrder(subClass);
			//System.out.println("size："+subClass.size());
			for(int i=0;i<subClass.size();i++)
			{
				Class<?> d = subClass.get(i);
				//反射调用方法
				driver = GodTool.newInstanceDataDriver(d);
				if(!driver.isSupport() || !driver.isSupport(param.getOperation()))
				{
					continue;
				}
				boolean f = driver.add(param);
				//********************************项目特别处理*************************************************//
				if(param.getProjectname() != null && !"".equalsIgnoreCase(param.getProjectname()) && once)
				{
					List<Class<?>> appClasss = ClassUtil.getClasses("com.idata.apps", IApplication.class);
					//System.out.println("size："+appClasss.size());
					for(int j=0;j<appClasss.size();j++)
					{
						Class<?> p = appClasss.get(j);
						if(p.getName().contains(param.getProjectname().toUpperCase()+"Application"))
						{
							IApplication app = (IApplication) GodTool.newInstance(p);
							boolean a = app.process(param,true);
							if(a)
							{
								once = false;
							}
							else
							{
								LogUtil.info(param.getProjectname()+"特别处理失败！");
							}
							break;
						}
					}
				}
				//*******************************************************************************************//
				if(!f)
				{
					flag = false;
				}
			}
			if(flag)
			{
				this.cleanCountCache(param);
				result = "{\"state\":true,\"message\":\"数据新增成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"数据新增失败\",\"size\":0,\"data\":[]}";
			}
		}
		else if("adds".equalsIgnoreCase(param.getOperation()))
		{
			boolean flag = true;
			List<Class<?>> subClass = ClassUtil.getClasses("com.idata.data", IDataDriver.class);
			classOrder(subClass);
			for(int i=0;i<subClass.size();i++)
			{
				Class<?> d = subClass.get(i);
				//反射调用方法
				driver = GodTool.newInstanceDataDriver(d);
				if(!driver.isSupport() || !driver.isSupport(param.getOperation()))
				{
					continue;
				}
				boolean f = driver.adds(param);
				if(!f)
				{
					flag = false;
				}
			}
			if(flag)
			{
				this.cleanCountCache(param);
				result = "{\"state\":true,\"message\":\"数据新增成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"数据新增失败\",\"size\":0,\"data\":[]}";
			}
		}
		else if("edit".equalsIgnoreCase(param.getOperation()))
		{
			boolean flag = true;
			List<Class<?>> subClass = ClassUtil.getClasses("com.idata.data", IDataDriver.class);
			classOrder(subClass);
			for(int i=0;i<subClass.size();i++)
			{
				Class<?> d = subClass.get(i);
				//反射调用方法
				driver = GodTool.newInstanceDataDriver(d);
				if(!driver.isSupport() || !driver.isSupport(param.getOperation()))
				{
					continue;
				}
				boolean f = driver.edit(param);
				if(!f)
				{
					flag = false;
				}
			}
			if(flag)
			{
				this.cleanCountCache(param);
				result = "{\"state\":true,\"message\":\"数据编辑成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"数据编辑失败\",\"size\":0,\"data\":[]}";
			}
		}
		else if("delete".equalsIgnoreCase(param.getOperation()))
		{
			boolean flag = true;
			List<Class<?>> subClass = ClassUtil.getClasses("com.idata.data", IDataDriver.class);
			classOrder(subClass);
			for(int i=0;i<subClass.size();i++)
			{
				Class<?> d = subClass.get(i);
				//反射调用方法
				driver = GodTool.newInstanceDataDriver(d);
				if(!driver.isSupport()  || !driver.isSupport(param.getOperation()))
				{
					continue;
				}
				boolean f = driver.delete(param);
				if(!f)
				{
					flag = false;
				}
			}
			if(flag)
			{
				this.cleanCountCache(param);
				result = "{\"state\":true,\"message\":\"数据删除成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"数据删除失败\",\"size\":0,\"data\":[]}";
			}
		}
		
		return result;
	}
	
	public void cleanCountCache(DataParam param)
	{
		//IDataDriver.resultSize.print();
		//System.out.println(param.getUid()+"&"+param.getCon()+"&"+param.getLayer()+"&");
		String key = param.getkey();
		IDataDriver.resultSize.removeContainKey(key);
		OneDataServer.resultcache.removeContainKey(key);
		//IDataDriver.resultSize.print();
	}
	
	private boolean checkIndexParam(DataParam param)
	{
		if(param.getUid() != null && !"".equalsIgnoreCase(param.getUid()))
		{
			String sql = "select indexpath,indexserver from "+OneDataServer.TablePrefix+"tableinfo where id='"+param.getUid()+"'";
			List<SuperObject> os = OneDataServer.SystemDBHandle.exeSQLSelect2(sql, 1, 1, null, null);
			if(os != null)
			{
				SuperObject o = os.get(0);
				String path = o.getProperty("indexpath").getString_value();
				param.setIndexPath(path);
				String indexServer = o.getProperty("indexserver").getString_value();
				param.setServer(indexServer);
				if(OneDataServer.CurrentServerNode.equalsIgnoreCase(indexServer))
				{
					return true;
				}
			}
			return false;
		}
		else
		{
			String sql = "select indexpath,indexserver from "+OneDataServer.TablePrefix+"tableinfo where con='"+param.getCon()+"' and layer='"+param.getLayer()+"'";
			List<SuperObject> os = OneDataServer.SystemDBHandle.exeSQLSelect2(sql, 1, 1, null, null);
			if(os != null)
			{
				SuperObject o = os.get(0);
				String path = o.getProperty("indexpath").getString_value();
				param.setIndexPath(path);
				String indexServer = o.getProperty("indexserver").getString_value();
				param.setServer(indexServer);
				if(OneDataServer.CurrentServerNode.equalsIgnoreCase(indexServer))
				{
					return true;
				}
			}
			return false;
		}
	}
	
	private String requestMain(DataParam param)
	{
		// 不是搜索节点，转发请求
		String url = "http://" + param.getServer() + "/Data?";
		String result = HttpRequestUtil.sendPost(url, param.toString());
		if(result == null)
		{
			result= "{\"state\":false,\"message\":\"出现内部错误，请联系管理员！\",\"size\":0,\"data\":[]}";
		}
		return result;
	}
	
	private void classOrder(List<Class<?>> subClass)
	{
		//排序
		Collections.sort(subClass, new Comparator<Class<?>>() {
		    @Override
		    public int compare(Class<?> o1, Class<?> o2) {
		    	IDataDriver driver1 = GodTool.newInstanceDataDriver(o1);
		    	IDataDriver driver2 = GodTool.newInstanceDataDriver(o2);
		        if(driver1.getClassOrder() > driver2.getClassOrder())
		        {
		        	return 1;
		        }
		        else if(driver1.getClassOrder() < driver2.getClassOrder())
		        {
		        	return -1;
		        }
		        else
		        {
		        	return 0;
		        }
		    }
		});
	}
	
	public String getFileNames(DataParam param)
	{
		String result = "";
		String fileNames = PropertiesUtil.getValue("FILE_NAME_FIELDS");
		if(fileNames.contains(","))
		{
			String[] names = fileNames.split(",");
			int size = names.length;
			for(int i=0;i<size;i++)
			{
				String fieldname = names[i];
				String fileName = "";
				if(param.getJsonobject().has(fileNames))
				{
					fileName = param.getJsonDataValue(fieldname).getString_value();
				}
				
				if("".equalsIgnoreCase(fileName) || !fileName.contains("."))
				{
					continue;
				}
				if("".equalsIgnoreCase(result))
				{
					result = fileName;
				}
				else
				{
					result += ","+fileName;
				}
			}
		}
		else
		{
			if(param.getJsonobject().has(fileNames))
			{
				result = param.getJsonobject().get(fileNames).getAsString();
			}
		}
		return result;
		
	}
	

}
