/**
 * ClassName:RegInterfaceControl.java
 * Date:2020年1月28日
 */
package com.idata.cotrol;

import java.util.List;

import com.idata.core.DataBaseHandle;
import com.idata.core.DataParam;
import com.idata.core.HbaseManager;
import com.idata.core.OneDataServer;
import com.idata.core.ResultBuilder;
import com.idata.core.SuperObject;
import com.idata.core.SystemManager;
import com.idata.core.TableIndexManager;
import com.idata.data.DataBaseDriver;
import com.idata.data.IDataDriver;
import com.ojdbc.sql.Value;

/**
 * Creater:SHAO Gaige
 * Description:注册接口信息访问控制器
 * Log:
 */
public class RegInterfaceControl {
	
	public String process(String type,String operation,String data,String field, String value,int start,int count)
	{
		DataBaseDriver dbDriver = new DataBaseDriver();
		dbDriver.setDataBaseHandle(OneDataServer.SystemDBHandle);
		
		DataParam param = new DataParam();
		param.setOperation(operation);
		param.setJsondata(data);
		param.setQueryfields(field);
		param.setQueryvalues(value);
		param.setStart(start);
		param.setCount(count);
		param.setLayer(OneDataServer.TablePrefix+"tableinfo");
		
		DataControl dataCon = new DataControl();
		
		String result = "";
		
		if("query".equalsIgnoreCase(param.getOperation()))
		{
			List<SuperObject> os = dbDriver.query(param);
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
			List<SuperObject> rsm = dbDriver.getMeta(param);
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
		else if("add".equalsIgnoreCase(param.getOperation()))
		{
			//内部处理
			SuperObject temp = new SuperObject();
			temp.setJSONString(param.getJsondata(), param.getIdfield(), param.getGeofield(),true);
			
			//系统自动生成
			temp.addProperty("indexserver", new Value().setString_value(OneDataServer.CurrentServerNode));
			temp.addProperty("register_time", new Value().setString_value(OneDataServer.getCurrentTime()));
			temp.addProperty("indexpath", new Value().setString_value(SystemManager.getTableIndexPath(param.getLayer())));
			if(temp.getProperty("con").getString_value().contains(","))
			{
				ConnectionControl con = new ConnectionControl();
				String con_s = con.getEncodeConStr(temp.getProperty("con").getString_value());
				temp.addProperty("con", new Value().setString_value(con_s));
			}
			DataBaseHandle dbh = dbDriver.getDBHandle(param);
			if(dbh == null)
			{
				return "{\"state\":false,\"message\":\"数据新增失败,数据库连接字符串不可用\",\"size\":0,\"data\":[]}";
			}
			else
			{
				temp.addProperty("status", new Value().setString_value("true"));
			}
			
			param.setJsondata(temp.getJSONString("json", param.getGeofield()));
			boolean flag = dbDriver.add(param);
			if(flag)
			{
				if(temp.getProperty("support").getString_value().contains("index"))
				{
					TableIndexManager.createTableIndex(temp.getProperty("con").getString_value(), temp.getProperty("layer").getString_value(),
							temp.getProperty("geofiled").getString_value(),temp.getProperty("indexpath").getString_value());
				}
				if(temp.getProperty("support").getString_value().contains("hbase"))
				{
					HbaseManager.importDataByThread(temp.getProperty("con").getString_value(), temp.getProperty("layer").getString_value(), 
							temp.getProperty("idfiled").getString_value(), temp.getProperty("geofiled").getString_value(), temp.getProperty("hbasepath").getString_value(), true);
				}
				dataCon.cleanCountCache(param);
				result = "{\"state\":true,\"message\":\"数据新增成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"数据新增失败\",\"size\":0,\"data\":[]}";
			}
		}
		else if("edit".equalsIgnoreCase(param.getOperation()))
		{
			boolean flag = dbDriver.edit(param);
			if(flag)
			{
				dataCon.cleanCountCache(param);
				result = "{\"state\":true,\"message\":\"数据编辑成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"数据编辑失败\",\"size\":0,\"data\":[]}";
			}
		}
		else if("delete".equalsIgnoreCase(param.getOperation()))
		{
			boolean flag = dbDriver.delete(param);
			if(flag)
			{
				dataCon.cleanCountCache(param);
				result = "{\"state\":true,\"message\":\"数据删除成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"数据删除失败\",\"size\":0,\"data\":[]}";
			}
		}
		
		return result;
	}
	
	

}
