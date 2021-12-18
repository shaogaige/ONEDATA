/**
 * ClassName:RegInterfaceControl.java
 * Date:2020年1月28日
 */
package com.idata.cotrol;

import com.idata.core.DataParam;
import com.idata.core.HbaseManager;
import com.idata.core.OneDataServer;
import com.idata.core.SystemManager;
import com.idata.core.TableIndexManager;
import com.ojdbc.sql.Value;

/**
 * Creater:SHAO Gaige
 * Description:内部信息访问控制器
 * Log:
 */
public class ProcessControl {
	
	public String process(DataParam param)
	{
		String result = "";
		param.setCon(OneDataServer.SystemDBHandle.getEncodeConStr());
		if("onedata_visit".equalsIgnoreCase(param.getLayer()) ||
				"onedata_datavisit".equalsIgnoreCase(param.getLayer()))
		{
			param.setCon(OneDataServer.SQLITEDBHandle.getEncodeConStr());
		}
		
		DataControl dataCon = new DataControl();
		
		if((OneDataServer.TablePrefix+"databaseinfo").equalsIgnoreCase(param.getLayer()))
		{
			dataCon = new ConnectionControl();
		}
		
		if((OneDataServer.TablePrefix+"tableinfo").equalsIgnoreCase(param.getLayer()))
		{
			if("add".equalsIgnoreCase(param.getOperation()))
			{
				//系统自动生成
				param.setJsonDataProperty("indexserver", new Value().setString_value(OneDataServer.CurrentServerNode));
				param.setJsonDataProperty("register_time", new Value().setString_value(OneDataServer.getCurrentTime()));
				param.setJsonDataProperty("indexpath", new Value().setString_value(SystemManager.getTableIndexPath(param.getLayer())));
				
				if(param.getJsonDataValue("support").getString_value().contains("index"))
				{
					TableIndexManager.createTableIndex(param.getJsonDataValue("con").getString_value(), param.getJsonDataValue("layer").getString_value(),
							param.getJsonDataValue("geofiled").getString_value(),param.getJsonDataValue("indexpath").getString_value());
				}
				if(param.getJsonDataValue("support").getString_value().contains("hbase"))
				{
					HbaseManager.importDataByThread(param.getJsonDataValue("con").getString_value(), param.getJsonDataValue("layer").getString_value(), 
							param.getJsonDataValue("idfiled").getString_value(),param.getJsonDataValue("geofiled").getString_value(), param.getJsonDataValue("hbasepath").getString_value(), true);
				}
			}
		}
		
		result = dataCon.process(param);
		return result;
	}
	
	

}
