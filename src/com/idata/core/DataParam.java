/**
 * ClassName:DataParam.java 
 * Date:2020年1月19日
 */
package com.idata.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ojdbc.sql.Value;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;

/**
 * Creater:SHAO Gaige
 * Description:data接口参数模型
 * Log:
 */
public class DataParam {
	
	//唯一标识
	private String uid = "";
	//数据库连接字符串
	private String con = "";
	//数据库表名
	private String layer = "";
	//数据操作
	private String operation = "query";
	//分页
	private String start_s = "";
	private String count_s = "";
	private int start = 1;
	private int count = Integer.MAX_VALUE;
	//实体数据
	private String jsondata = "";
	private JsonObject jsonobject = null;
	//输出字段
	private String outfields = "";
	//查询字段名称
	private String queryfields = "";
	//查询操作
	private String queryoperates = "";
	//关键字
	private String queryvalues = "";
	//查询操作关系
	private String queryrelations = "";
	//空间范围
	private String bbox = "";
	private Geometry filterGeometry = null;
	//ID字段
	private String idfield = "";
	//空间字段
	private String geofield = "";
	//空间操作
	private String geoaction = "intersects";
	//自定义SQL
	private String usersql = "";
	//排序字段
	private String orderfields = "";
	//求和字段
	private String sumfield = "";
	//分组字段
	private String groupfield = "";
	//数据存储类型
	private String type = "table";
	//输出格式
	private String out = "json";
	//访问授权
	private String token = "";
	//时间
	private String time = "";
	
	//请求字符串
	private String param_String = "";
	//路径
	private List<String> path = new ArrayList<String>();
	//路径,索引使用
	private String indexpath = "";
	//服务节点,索引使用
	private String server = "";
	//文件存储
	private Map<String,byte[]> files = new LinkedHashMap<String,byte[]>();
	//文件名称
	private String filenames = "";
	//项目名称
	private String projectname = "";
	
	private boolean isTokenValid()
	{
		return true;
	}
	//错误信息内容
	private String message = "请求参数出错！";
	public boolean isValid()
	{
		if((uid == null || "".equalsIgnoreCase(uid)) && 
			((con == null || "".equalsIgnoreCase(con)) &&
			 (layer == null || "".equalsIgnoreCase(layer))))
		{
			this.message = "参数[id]和参数[con][layer]不能同时为空！";
			return false;
		}
		if(operation == null || "".equalsIgnoreCase(operation))
		{
			this.message = "参数[operation]不能为空！";
			return false;
		}
		else if(!"query".equalsIgnoreCase(operation) && !"getmeta".equalsIgnoreCase(operation) &&
		  !"add".equalsIgnoreCase(operation) && !"edit".equalsIgnoreCase(operation) &&
		  !"delete".equalsIgnoreCase(operation) && !"group".equalsIgnoreCase(operation))
		{
			this.message = "参数[operation]值错误！";
			return false;
		}
		
		if(jsondata !=null && !"".equalsIgnoreCase(jsondata))
		{
			try 
			{
				//jsonobject = new JsonParser().parse(jsondata).getAsJsonObject();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				this.message = "参数[jsondata]值错误！";
				return false;
			}
		}
		
		if(type == null || "".equalsIgnoreCase(type))
		{
			this.message = "参数[type]不能为空！";
			return false;
		}
		else if(!"table".equalsIgnoreCase(type) && !"index".equalsIgnoreCase(type) &&
				!"hbase".equalsIgnoreCase(type))
		{
			this.message = "参数[type]值错误！";
			return false;
		}
		
		if(!"json".equalsIgnoreCase(out) && !"geojson".equalsIgnoreCase(out))
		{
			this.message = "参数[out]值错误！";
			return false;
		}
		
		if(!isTokenValid())
		{
			this.message = "参数[token]值错误！";
			return false;
		}
		
		return true;
	}
	public JsonObject getJsonobject() {
		if(jsonobject == null)
		{
			try {
				jsonobject = new JsonParser().parse(jsondata).getAsJsonObject();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				this.message = "参数[jsondata]值错误！";
			}
		}
		return jsonobject;
	}
	public JsonArray getJsonArray() {
		try 
		{
			//System.out.println(jsondata);
			JsonArray jsonarray = new JsonParser().parse(jsondata).getAsJsonArray();
			return jsonarray;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			this.message = "参数[jsondata]值错误！";
			return new JsonArray();
		}
	}
	
	public String getMessage() {
		return message;
	}

	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getCon() {
		return con;
	}
	public void setCon(String con) {
		this.con = con;
	}
	public String getLayer() {
		return layer;
	}
	public void setLayer(String layer) {
		this.layer = layer;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		if(operation != null && !"".equalsIgnoreCase(operation))
		{
			this.operation = operation;
		}
	}
	public String getStart_s() {
		return start_s;
	}
	public void setStart_s(String start_s) {
		this.start_s = start_s;
		try
		{
			if(this.start_s != null && !"".equalsIgnoreCase(this.start_s)) {
				this.start = Integer.parseInt(this.start_s.trim());
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	public String getCount_s() {
		return count_s;
	}
	public void setCount_s(String count_s) {
		this.count_s = count_s;
		try
		{
			if(this.count_s != null && !"".equalsIgnoreCase(this.count_s)) {
				this.count = Integer.parseInt(this.count_s.trim());
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public String getJsondata() {
		return jsondata;
	}
	public void setJsondata(String jsondata) {
		this.jsondata = jsondata;
	}
	
	public String getQueryvalues() {
		return queryvalues;
	}
	public void setQueryvalues(String queryvalues) {
		this.queryvalues = queryvalues;
	}
	public String getQueryrelations() {
		return queryrelations;
	}
	public void setQueryrelations(String queryrelations) {
		this.queryrelations = queryrelations;
	}
	public String getQueryfields() {
		return queryfields;
	}
	public void setQueryfields(String queryfields) {
		this.queryfields = queryfields;
	}
	public String getQueryoperates() {
		return queryoperates;
	}
	public void setQueryoperates(String queryoperates) {
		this.queryoperates = queryoperates;
	}
	public String getOutfields() {
		return outfields;
	}
	public void setOutFields(String outfields) {
		this.outfields = outfields;
	}
	public String getBbox() {
		return bbox;
	}
	public void setBbox(String bbox) {
		this.bbox = bbox;
	}
	public Geometry getFilterGeometry() {
		return filterGeometry;
	}
	public void setFilterGeometry(Geometry filterGeometry) {
		this.filterGeometry = filterGeometry;
	}
	public PreparedGeometry getFilterPreparedGeometry() {
		if(this.filterGeometry != null) {
			return PreparedGeometryFactory.prepare(this.filterGeometry);
		}
		return null;
	}
	public String getIdfield() {
		return idfield;
	}
	public void setIdfield(String idfield) {
		if(idfield != null && !"".equalsIgnoreCase(idfield))
		{
			this.idfield = idfield;
		}
	}
	public String getGeofield() {
		return geofield;
	}
	public void setGeofield(String geofield) {
		if(geofield != null && !"".equalsIgnoreCase(geofield))
		{
			this.geofield = geofield;
		}
	}
	public String getGeoaction() {
		return geoaction;
	}
	public void setGeoaction(String geoaction) {
		if(geoaction != null && !"".equalsIgnoreCase(geoaction))
		{
			this.geoaction = geoaction;
		}
	}
	public String getUsersql() {
		return usersql;
	}
	public void setUsersql(String usersql) {
		this.usersql = usersql;
	}
	public String getSumfield() {
		return sumfield;
	}
	public void setSumfield(String sumfield) {
		this.sumfield = sumfield;
	}
	public String getGroupfield() {
		return groupfield;
	}
	public void setGroupfield(String groupfield) {
		this.groupfield = groupfield;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		if(type != null && !"".equalsIgnoreCase(type))
		{
			this.type = type;
		}
	}
	public String getOut() {
		return out;
	}
	public void setOut(String out) {
		if(out != null && !"".equalsIgnoreCase(out))
		{
			this.out = out;
		}
	}
	public String getToken() {
		return token;
	}
	public void setToken(String token) {
		this.token = token;
	}
	
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public void setParam_String(String param_String) {
		this.param_String = param_String;
	}
	public String getParam_String() {
		return param_String;
	}
	public List<String> getPath() {
		return path;
	}
	public void addPath(String path) {
		if(path != null && !"".equalsIgnoreCase(path))
		{
			this.path.add(path);
		}
	}
	public String getIndexPath() {
		return indexpath;
	}
	public void setIndexPath(String indexpath) {
		this.indexpath = indexpath;
	}
	public String getServer() {
		return server;
	}
	public void setServer(String server) {
		if(server != null && !"".equalsIgnoreCase(server))
		{
			this.server = server;
		}
	}
	public String getOrderfields() {
		return orderfields;
	}
	public void setOrderfields(String orderfields) {
		this.orderfields = orderfields;
	}
	
	public String getkey()
	{
		String key = toString();
		if(this.uid != null && !"".equalsIgnoreCase(this.uid))
		{
			key = uid;
		}
		else if(this.con != null && !"".equalsIgnoreCase(this.con))
		{
			key = con;
		}
		
		if(!key.contains("&"))
		{
			key += "&"+layer+"&";
		}
		return key;
	}
	
	public String toString()
	{
		return this.uid+"&"+this.layer+"&"+this.con+"&"+this.operation+"&"+this.start_s+"&"+this.count_s+"&"+this.outfields+
					"&"+this.queryfields+"&"+this.queryoperates+"&"+this.queryvalues+"&"+this.queryrelations+"&"+this.bbox+"&"+this.idfield+
					"&"+this.geofield+"&"+this.geoaction+"&"+this.usersql+"&"+this.orderfields+"&"+this.sumfield+"&"+this.groupfield+"&"+this.type+"&"+this.out+
					"&"+this.token+"&"+this.time;
	}
	
	public String getJsonString()
	{
		SuperObject so = new SuperObject();
		so.addProperty("uid", new Value().setString_value(this.uid));
		so.addProperty("con", new Value().setString_value(this.con));
		so.addProperty("layer", new Value().setString_value(this.layer));
		so.addProperty("operation", new Value().setString_value(this.operation));
		so.addProperty("start", new Value().setInt_value(this.start));
		so.addProperty("count", new Value().setInt_value(this.count));
		so.addProperty("jsondata", new Value().setString_value(this.jsondata));
		so.addProperty("outfields", new Value().setString_value(this.outfields));
		so.addProperty("queryfields", new Value().setString_value(this.queryfields));
		so.addProperty("queryoperates", new Value().setString_value(this.queryoperates));
		so.addProperty("queryvalues", new Value().setString_value(this.queryvalues));
		so.addProperty("queryrelations", new Value().setString_value(this.queryrelations));
		so.addProperty("bbox", new Value().setString_value(this.bbox));
		so.addProperty("idfield", new Value().setString_value(this.idfield));
		so.addProperty("geofield", new Value().setString_value(this.geofield));
		so.addProperty("geoaction", new Value().setString_value(this.geoaction));
		so.addProperty("usersql", new Value().setString_value(this.usersql));
		so.addProperty("orderfields", new Value().setString_value(this.orderfields));
		so.addProperty("sumfield", new Value().setString_value(this.sumfield));
		so.addProperty("groupfield", new Value().setString_value(this.groupfield));
		so.addProperty("type", new Value().setString_value(this.type));
		so.addProperty("out", new Value().setString_value(this.out));
		so.addProperty("token", new Value().setString_value(this.token));
		so.addProperty("time", new Value().setString_value(this.time));
		so.addProperty("indexpath", new Value().setString_value(this.indexpath));
		so.addProperty("server", new Value().setString_value(this.server));
		return so.getJSONString("json", this.geofield);
	}
	
	public DataParam copy()
	{
		DataParam p = new DataParam();
		p.setUid(uid);
		p.setCon(con);
		p.setLayer(layer);
		p.setOperation(operation);
		p.setStart_s(start_s);
		p.setCount_s(count_s);
		p.setJsondata(jsondata);
		p.setOutFields(outfields);
		p.setQueryfields(queryfields);
		p.setQueryoperates(queryoperates);
		p.setQueryvalues(queryvalues);
		p.setQueryrelations(queryrelations);
		p.setBbox(bbox);
		p.setIdfield(idfield);
		p.setGeofield(geofield);
		p.setGeoaction(geoaction);
		p.setUsersql(usersql);
		p.setOrderfields(orderfields);
		p.setSumfield(sumfield);
		p.setGroupfield(groupfield);
		p.setType(type);
		p.setOut(out);
		p.setToken(token);
		p.setTime(time);
		p.setParam_String(param_String);
		p.setIndexPath(indexpath);
		p.setServer(server);
		p.setFilenames(filenames);
		p.setProjectname(projectname);
		p.path = path;
		p.files = files;
		return p;
	}
	
	public void setJsonString(String jsonstr)
	{
		SuperObject so = new SuperObject();
		so.setJSONString(jsonstr, null, null, true);
		this.setUid(so.getProperty("uid").getString_value());
		this.setCon(so.getProperty("con").getString_value());
		this.setLayer(so.getProperty("layer").getString_value());
		this.setOperation(so.getProperty("operation").getString_value());
		this.setStart(so.getProperty("start").getInt_value());
		this.setCount(so.getProperty("count").getInt_value());
		this.setJsondata(so.getProperty("jsondata").getString_value());
		this.setOutFields(so.getProperty("outfields").getString_value());
		this.setQueryfields(so.getProperty("queryfields").getString_value());
		this.setQueryoperates(so.getProperty("queryoperates").getString_value());
		this.setQueryvalues(so.getProperty("queryvalues").getString_value());
		this.setQueryrelations(so.getProperty("queryrelations").getString_value());
		this.setBbox(so.getProperty("bbox").getString_value());
		this.setIdfield(so.getProperty("idfield").getString_value());
		this.setGeofield(so.getProperty("geofield").getString_value());
		this.setGeoaction(so.getProperty("geoaction").getString_value());
		this.setUsersql(so.getProperty("usersql").getString_value());
		this.setOrderfields(so.getProperty("orderfields").getString_value());
		this.setSumfield(so.getProperty("sumfield").getString_value());
		this.setGroupfield(so.getProperty("groupfield").getString_value());
		this.setType(so.getProperty("type").getString_value());
		this.setOut(so.getProperty("out").getString_value());
		this.setToken(so.getProperty("token").getString_value());
		this.setTime(so.getProperty("time").getString_value());
		this.setIndexPath(so.getProperty("indexpath").getString_value());
		this.setServer(so.getProperty("server").getString_value());
	}
	
	public Map<String, byte[]> getFiles() {
		return files;
	}
	
	public void addFile(String fileName,byte[] file)
	{
		this.files.put(fileName, file);
	}
	
	public String getFilenames() {
		return filenames;
	}
	public void setFilenames(String filenames) {
		this.filenames = filenames;
	}
	
	public String getProjectname() {
		return projectname;
	}
	public void setProjectname(String projectname) {
		this.projectname = projectname;
	}
	public Value getJsonDataValue(String field)
	{
		if(this.jsonobject == null)
		{
			try {
				this.jsonobject = new JsonParser().parse(jsondata).getAsJsonObject();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				this.message = "参数[jsondata]值错误！";
			}
		}
		if(this.jsonobject == null || JsonNull.INSTANCE.equals(this.jsonobject.get(field)) || null==this.jsonobject.get(field))
		{
			return new Value();
		}
		if(this.jsonobject.get(field).getAsJsonPrimitive().isString())
		{
			return new Value().setString_value(this.jsonobject.get(field).getAsString());
		}
		else if(this.jsonobject.get(field).getAsJsonPrimitive().isNumber())
		{
			return new Value().setDouble_value(this.jsonobject.get(field).getAsDouble());
			//this.properties.put(key, new Value().setDouble_value(Double.parseDouble(entry.getValue().getAsNumber().toString())));
		}
		else if(this.jsonobject.get(field).getAsJsonPrimitive().isBoolean())
		{
			return new Value().setBoolean_value(this.jsonobject.get(field).getAsBoolean());
		}
		else
		{
			return new Value().setString_value(this.jsonobject.get(field).getAsString());
		}
	}
	
	public void setJsonDataProperty(String field,Value v)
	{
		if(this.jsonobject == null)
		{
			if(this.jsondata == null || "".equalsIgnoreCase(this.jsondata))
			{
				this.jsonobject = new JsonObject();
			}
			else
			{
				try {
					this.jsonobject = new JsonParser().parse(jsondata).getAsJsonObject();
				}
				catch(Exception e)
				{
					e.printStackTrace();
					this.message = "参数[jsondata]值错误！";
				}
			}
		}
		if(v.isStringValue())
		{
			this.jsonobject.addProperty(field, v.getString_value());
		}
		else if(v.isDoubleValue() || v.isFloatValue())
		{
			this.jsonobject.addProperty(field, v.getDouble_value());
		}
		else if(v.isIntValue())
		{
			this.jsonobject.addProperty(field, v.getInt_value());
		}
		else if(v.isLongValue())
		{
			this.jsonobject.addProperty(field, v.getLong_value());
		}
		else if(v.isBooleanValue())
		{
			this.jsonobject.addProperty(field, v.getBoolean_value());
		}
		else
		{
			this.jsonobject.addProperty(field, v.getString_value());
		}
		this.jsondata = this.jsonobject.toString();
	}
	

}
