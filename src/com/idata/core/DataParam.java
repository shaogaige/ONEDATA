/**
 * ClassName:DataParam.java 
 * Date:2020年1月19日
 */
package com.idata.core;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
	private String operation = "";
	//分页
	private String start_s = "";
	private String count_s = "";
	private int start = 1;
	private int count = Integer.MAX_VALUE;
	//实体数据
	private String jsondata = "";
	private JsonObject jsonobject = null;
	//输出字段
	private String outFields = "";
	//查询字段名称
	private String queryfields = "";
	//查询操作
	private String queryoperates = "";
	//关键字
	private String keywords = "";
	//查询操作关系
	private String queryrelations = "";
	//空间范围
	private String bbox = "";
	private Geometry filterGeometry = null;
	//ID字段
	private String idfield = null;
	//空间字段
	private String geofield = null;
	//空间操作
	private String geoaction = "intersects";
	//自定义SQL
	private String usersql = "";
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
	private String time;
	
	//请求字符串
	private String param_String = null;
	//路径,索引使用
	private String path = null;
	//服务节点,索引使用
	private String server = null;
	
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
			try {
				jsonobject = new JsonParser().parse(jsondata).getAsJsonObject();
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
			this.message = "参数[operation]不能为空！";
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
		return jsonobject;
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
		this.operation = operation;
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
	
	public String getKeywords() {
		return keywords;
	}
	public void setKeywords(String keywords) {
		this.keywords = keywords;
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
	public String getOutFields() {
		return outFields;
	}
	public void setOutFields(String outFields) {
		this.outFields = outFields;
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
		this.idfield = idfield;
	}
	public String getGeofield() {
		return geofield;
	}
	public void setGeofield(String geofield) {
		this.geofield = geofield;
	}
	public String getGeoaction() {
		return geoaction;
	}
	public void setGeoaction(String geoaction) {
		this.geoaction = geoaction;
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
		this.type = type;
	}
	public String getOut() {
		return out;
	}
	public void setOut(String out) {
		this.out = out;
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
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getServer() {
		return server;
	}
	public void setServer(String server) {
		this.server = server;
	}
	public String toString()
	{
		return this.param_String;
	}

}
