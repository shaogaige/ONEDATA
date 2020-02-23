/**
 * ClassName:DataControl.java
 * Date:2020年1月28日
 */
package com.idata.cotrol;

import java.util.Iterator;
import java.util.List;

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
				Long size = IDataDriver.resultSize.get(param.toString());
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
			List<Class<?>> subClass = ClassUtil.getClasses("com.idata.data", IDataDriver.class);
			for(int i=0;i<subClass.size();i++)
			{
				Class<?> d = subClass.get(i);
				//反射调用方法
				driver = GodTool.newInstanceDataDriver(d);
				if(!driver.isSupport())
				{
					continue;
				}
				boolean f = driver.add(param);
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
			for(int i=0;i<subClass.size();i++)
			{
				Class<?> d = subClass.get(i);
				//反射调用方法
				driver = GodTool.newInstanceDataDriver(d);
				if(!driver.isSupport())
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
			for(int i=0;i<subClass.size();i++)
			{
				Class<?> d = subClass.get(i);
				//反射调用方法
				driver = GodTool.newInstanceDataDriver(d);
				if(!driver.isSupport())
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
		if(IDataDriver.resultSize.containsKey(param.toString()))
		{
			for(Iterator<String> iterator = IDataDriver.resultSize.keySet().iterator(); iterator.hasNext(); ) 
			{
				String key = iterator.next();
				if(key.equalsIgnoreCase(param.toString())) 
				{
					iterator.remove();
				}
			}
		}
	}
	
	private boolean checkIndexParam(DataParam param)
	{
		if(param.getUid() != null && !"".equalsIgnoreCase(param.getUid()))
		{
			String sql = "select indexpath,indexserver from "+OneDataServer.TablePrefix+"datainfo where id='"+param.getUid()+"'";
			List<SuperObject> os = OneDataServer.SystemDBHandle.exeSQLSelect2(sql, 1, 1, null, null);
			if(os != null)
			{
				SuperObject o = os.get(0);
				String path = o.getProperty("indexpath").getString_value();
				param.setPath(path);
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
			String sql = "select indexpath,indexserver from "+OneDataServer.TablePrefix+"datainfo where con='"+param.getCon()+"' and layer='"+param.getLayer()+"'";
			List<SuperObject> os = OneDataServer.SystemDBHandle.exeSQLSelect2(sql, 1, 1, null, null);
			if(os != null)
			{
				SuperObject o = os.get(0);
				String path = o.getProperty("indexpath").getString_value();
				param.setPath(path);
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
		String url = "http://" + param.getServer() + "/Data";
		String result = HttpRequestUtil.sendPost(url, param.toString());
		if(result == null)
		{
			result= "{\"state\":false,\"message\":\"出现内部错误，请联系管理员！\",\"size\":0,\"data\":[]}";
		}
		return result;
	}
	

}
