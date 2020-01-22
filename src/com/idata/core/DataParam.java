/**
 * ClassName:DataParam.java 
 * Date:2020年1月19日
 */
package com.idata.core;

/**
 * Creater:SHAO Gaige
 * Description:data接口参数模型
 * Log:
 */
public class DataParam {
	
	//唯一标识
	private String id = "";
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
	//字段名称
	private String field = "";
	//关键字
	private String keyword = "";
	//空间范围
	private String bbox = "";
	//空间字段
	private String geofield = "";
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
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
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
	public String getField() {
		return field;
	}
	public void setField(String field) {
		this.field = field;
	}
	public String getKeyword() {
		return keyword;
	}
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
	public String getBbox() {
		return bbox;
	}
	public void setBbox(String bbox) {
		this.bbox = bbox;
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

}
