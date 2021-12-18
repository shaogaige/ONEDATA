/**
 * ClassName:VisitControl.java
 * Date:2020年1月28日
 */
package com.idata.cotrol;

import java.util.Date;
import java.util.List;

import com.google.gson.Gson;
import com.idata.core.DataParam;
import com.idata.core.OneDataServer;
import com.idata.core.SuperObject;
import com.idata.data.DataBaseDriver;
import com.idata.tool.ActiveMQUtil;
import com.idata.tool.LogUtil;
import com.idata.tool.PropertiesUtil;
import com.idata.tool.RabbitMQUtil;
import com.idata.tool.TWBCacheManager;
import com.ojdbc.sql.Value;


/**
 * Creater:SHAO Gaige
 * Description:接口访问信息控制器
 * Log:
 */
public class VisitControl {
	
	private static Gson gson = new Gson();
	
	private static TWBCacheManager<SuperObject> datavisitcache = new TWBCacheManager<SuperObject>();
	
	public static void log(DataParam dataParam,boolean f)
	{
		if(!f)
		{
			return;
		}
		try 
		{
			if(OneDataServer.isActiveMQ)
			{
				ActiveMQUtil.sendMessage(dataParam.getJsonString());
			}
			else if(OneDataServer.isRabbitMQ)
			{
				RabbitMQUtil.sendMessage(dataParam.getJsonString());
			}
			else
			{
				DataControl dataCon = new DataControl();
				dataCon.process(dataParam);
			}
		} 
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void save(String data)
	{
		try
		{
			//判断是否已经存在
			DataParam dataparam = new DataParam();
			dataparam.setJsonString(data);
			//区分处理
			if(dataparam.getCon() != null)
			{
				if(dataparam.getCon().equalsIgnoreCase(OneDataServer.SQLITEDBHandle.getEncodeConStr()))
				{
					//系统表
					if("onedata_datavisit".equalsIgnoreCase(dataparam.getLayer()))
					{
						SuperObject result = null;
						DataBaseDriver dataDriver = new DataBaseDriver();
						if(VisitControl.datavisitcache.containsKey(dataparam.getQueryvalues()))
						{
							result = VisitControl.datavisitcache.getObject(dataparam.getQueryvalues());
						}
						else
						{
							List<SuperObject> rs = dataDriver.query(dataparam);
							if(rs != null && rs.size()>0)
							{
								result = rs.get(0);
								VisitControl.datavisitcache.add(dataparam.getQueryvalues(), result);
							}
						}
						
						if(result != null)
						{
							//已存在 启动更新过程
							long vcount = result.getProperty("visit_count").getLong_value();
							dataparam.setJsonDataProperty("visit_count", new Value().setLong_value(vcount+1));
							dataDriver.edit(dataparam);
							
							result.addProperty("visit_count", new Value().setLong_value(vcount+1));
							VisitControl.datavisitcache.add(dataparam.getQueryvalues(), result);
						}
						else
						{
							dataDriver.add(dataparam);
						}
						return;
					}
				}
			}
			//其它数据处理
			DataControl dataCon = new DataControl();
			dataCon.process(dataparam);
		}
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void log(DataParam param,Exception e,LogUtil.Level l,String interfacename)
	{
		VisitControl.WarnModel model = new VisitControl.WarnModel();
		model.setDatabase_id(param.getUid());
		String con = param.getCon();
		if(OneDataServer.databaseinfo.containsKey(param.getUid()))
		{
			con = OneDataServer.databaseinfo.getObject(param.getUid()).getProperty("database_conname").getString_value();
		}
		model.setDatabase_conname(con);
		model.setTable_name(param.getLayer());
		model.setInterface_name(interfacename);
		model.setInterface_type(param.getOperation());
		model.setWarn_type(e.getMessage());
		model.setWarn_level(l.getTag());
		model.setWarn_detail(e.toString());
		VisitControl.log(model);
	}
	
	public static void log(WarnModel model)
	{
		//构建统一参数
		DataParam dataParam = new DataParam();
		String jsondata = gson.toJson(model);
		dataParam.setCon(OneDataServer.SystemDBHandle.getEncodeConStr());
		dataParam.setLayer(OneDataServer.TablePrefix+"warninfo");
		dataParam.setOperation("add");
		dataParam.setJsondata(jsondata);
		//dataParam.setIdfield("id");
		//发送请求
		log(dataParam,true);
	}
	
	public static void log(VisitLogModel model)
	{
		if(!OneDataServer.visitlog)
		{
			return;
		}
		//构建统一参数
		DataParam dataParam = new DataParam();
		String jsondata = gson.toJson(model);
		dataParam.setCon(OneDataServer.SQLITEDBHandle.getEncodeConStr());
		dataParam.setLayer(OneDataServer.TablePrefix+"visit");
		dataParam.setOperation("add");
		dataParam.setJsondata(jsondata);
		dataParam.setIdfield("id");
		//发送请求
		log(dataParam,true);
	}
	
	public static class VisitLogModel {
		
		//private long id;
		
		private String req_time;
		
		//private long reqtime;
		
		private String req_ip;
		
		private String req_server = "onedata";
		
		private String req_project;
		
		private String req_databaseid;
		
		private String req_databasename;
		
		private String req_interfacename;

		private String req_interfacetype;
		
		private String req_layer;

		private String req_keyword = "";
		
		private String token = "";
		
		private String usertype = "";
		
		private String results = "";
		
		private String remarks = "";
		
		public VisitLogModel()
		{
			Date date = new Date();
			//reqtime = date.getTime();
			req_time = OneDataServer.df.format(date);
			req_server = PropertiesUtil.getValue("SERVERNAME");
		}

		//public long getId() {
		//	return id;
		//}

		//public void setId(long id) {
		//	this.id = id;
		//}

		public String getReq_time() {
			return req_time;
		}

		public void setReq_time(String req_time) {
			this.req_time = req_time;
		}

		public String getReq_ip() {
			return req_ip;
		}

		public void setReq_ip(String req_ip) {
			this.req_ip = req_ip;
		}

		public String getReq_server() {
			return req_server;
		}

		public void setReq_server(String req_server) {
			this.req_server = req_server;
		}
		
		public String getReq_project() {
			return req_project;
		}

		public void setReq_project(String req_project) {
			this.req_project = req_project;
		}
		
		public String getReq_layer() {
			return req_layer;
		}

		public void setReq_layer(String req_layer) {
			this.req_layer = req_layer;
		}

		public String getReq_interfacename() {
			return req_interfacename;
		}

		public void setReq_interfacename(String req_interfacename) {
			this.req_interfacename = req_interfacename;
		}

		public String getReq_databaseid() {
			return req_databaseid;
		}

		public void setReq_databaseid(String req_databaseid) {
			this.req_databaseid = req_databaseid;
		}

		public String getReq_databasename() {
			return req_databasename;
		}

		public void setReq_databasename(String req_databasename) {
			this.req_databasename = req_databasename;
		}

		public String getReq_interfacetype() {
			return req_interfacetype;
		}

		public void setReq_interfacetype(String req_interfacetype) {
			this.req_interfacetype = req_interfacetype;
		}

		public String getReq_keyword() {
			if(req_keyword.length()>150)
			{
				req_keyword = req_keyword.substring(0, 150);
			}
			return req_keyword;
		}

		public void setReq_keyword(String req_keyword) {
			if(req_keyword != null && !"".equalsIgnoreCase(req_keyword))
			{
				if(this.req_keyword != null && !"".equalsIgnoreCase(this.req_keyword))
				{
					this.req_keyword += ";";
				}
				this.req_keyword += req_keyword;
			}
		}

		public String getToken() {
			return token;
		}

		public void setToken(String token) {
			if(token != null && !"".equalsIgnoreCase(token))
			{
				this.token = token;
				this.usertype = token.substring(14, 15);
			}
		}

		public String getUsertype() {
			return usertype;
		}

		public void setUsertype(String usertype) {
			this.usertype = usertype;
		}

		public String getResults() {
			if(results.length()>100)
			{
				results = results.substring(0, 100);
			}
			return results;
		}

		public void setResults(String results) {
			if(results != null && results.length()>100)
			{
				results = results.substring(0, 100);
			}
			this.results = results;
		}

		public String getRemarks() {
			if(remarks.length()>50)
			{
				remarks = remarks.substring(0, 50);
			}
			return remarks;
		}

		public void setRemarks(String remarks) {
			this.remarks = remarks;
		}

		//public long getReqtime() {
		//	return reqtime;
		//}

	}
	
	
	public static class WarnModel {
		
		private String database_id;
		
		private String database_conname;
		
		private String table_name;
		
		private String interface_type;
		
		private String interface_name;
		
		private String warn_type;
		
		private String warn_level;
		
		private String warn_time;
		
		private String warn_detail;
		
		private String remark;
		
		public WarnModel()
		{
			this.warn_time = OneDataServer.getCurrentTime();
		}

		public String getDatabase_id() {
			return database_id;
		}

		public void setDatabase_id(String database_id) {
			this.database_id = database_id;
		}

		public String getDatabase_conname() {
			return database_conname;
		}

		public void setDatabase_conname(String database_conname) {
			this.database_conname = database_conname;
		}

		public String getTable_name() {
			return table_name;
		}

		public void setTable_name(String table_name) {
			this.table_name = table_name;
		}

		public String getInterface_type() {
			return interface_type;
		}

		public void setInterface_type(String interface_type) {
			this.interface_type = interface_type;
		}

		public String getInterface_name() {
			return interface_name;
		}

		public void setInterface_name(String interface_name) {
			this.interface_name = interface_name;
		}

		public String getWarn_type() {
			return warn_type;
		}

		public void setWarn_type(String warn_type) {
			this.warn_type = warn_type;
		}

		public String getWarn_level() {
			return warn_level;
		}

		public void setWarn_level(String warn_level) {
			this.warn_level = warn_level;
		}

		public String getWarn_time() {
			return warn_time;
		}

		public void setWarn_time(String warn_time) {
			this.warn_time = warn_time;
		}

		public String getWarn_detail() {
			return warn_detail;
		}

		public void setWarn_detail(String warn_detail) {
			this.warn_detail = warn_detail;
		}

		public String getRemark() {
			return remark;
		}

		public void setRemark(String remark) {
			this.remark = remark;
		}
		
	}

}
