/**
 * ClassName:SuperObject.java 
 * Date:2020年1月9日
 */
package com.idata.core;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.geotools.geojson.GeoJSON;
import org.geotools.geojson.geom.GeometryJSON;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Creater:SHAO Gaige
 * Description:统一对象模型
 * Log:
 */
public class SuperObject {
	//对象id
	private String oid = "";
	//类型：normal/spatial
	private String type = "normal";
	//属性信息
	private Map<String,Value> properties = new LinkedHashMap<String,Value>();
	//空间坐标数据
	private Geometry geo;
	private String geo_wkt = "";
	
	public String getOid() {
		return oid;
	}
	public void setOid(String oid) {
		this.oid = oid;
	}
	public String getType() {
		return type;
	}
	public Geometry getGeo() {
		if(geo != null)
		{
			return geo;
		}
		else
		{
			if(this.geo_wkt != null && !"".equalsIgnoreCase(geo_wkt))
			{
				try 
				{
					WKTReader OGCWKTReader = new WKTReader();
					return OGCWKTReader.read(geo_wkt);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			}
			return null;
		}
	}
	public void setGeo(Geometry geo) {
		this.geo = geo;
		this.type = "spatial";
	}
	public String getGeo_wkt() {
		if(this.geo_wkt != null && !"".equalsIgnoreCase(geo_wkt))
		{
			return geo_wkt;
		}
		else
		{
			if(this.geo != null)
			{
				return this.geo.toText();
			}
			return "";
		}
	}
	public void setGeo_wkt(String geo_wkt) {
		this.geo_wkt = geo_wkt;
		this.type = "spatial";
	}
	
	public void addProperty(String key,Value v)
	{
		this.properties.put(key, v);
	}
	
	public Set<String>  getKeys()
	{
		return this.properties.keySet();
	}
	
	public Value getProperty(String key)
	{
		return this.properties.get(key);
	}
	
	private JsonObject getJson()
	{
		JsonObject feature = new JsonObject();
		for(String key:this.properties.keySet())
		{
			Value v = this.properties.get(key);
			if(v.isStringValue())
			{
				feature.addProperty(key, v.getString_value());
			}
			else if(v.isDoubleValue())
			{
				feature.addProperty(key, v.getDouble_value());
			}
			else if(v.isBooleanValue())
			{
				feature.addProperty(key, v.getBoolean_value());
			}
			else
			{
				feature.addProperty(key, v.getString_value());
			}
		}
		return feature;
	}
	
	public JsonObject getJSONObject(String out)
	{
		JsonObject feature = new JsonObject();
		if("normal".equalsIgnoreCase(this.type))
		{
			feature = getJson();
		}
		else
		{
			if("geojson".equalsIgnoreCase(out))
			{
				JsonObject proper = getJson();
				feature.addProperty("type", "Feature");
				StringWriter output = new StringWriter();
				try {
					GeoJSON.write(geo, output);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				JsonObject jsongeo = new JsonParser().parse(output.toString()).getAsJsonObject();
				feature.add("geometry",jsongeo);
				feature.add("properties", proper);
			}
			else
			{
				feature = getJson();
				feature.addProperty("_geometry_", getGeo_wkt());
			}
		}
		return feature;
	}
	
	public String getJSONString(String out)
	{
		return getJSONObject(out).toString();
	}
	
	public void setJSONString(String jsons)
	{
		if(jsons==null || "".equalsIgnoreCase(jsons)) 
		{
			return ;
		}
		this.properties = new LinkedHashMap<String,Value>();
		JsonObject contents=new JsonParser().parse(jsons).getAsJsonObject();
		if(contents.has("type") && "Feature".equalsIgnoreCase(contents.get("type").getAsString()))
		{
			this.type = "spatial";
			//geojson
			String gjson = contents.get("geometry").toString();
			StringReader input = new StringReader(gjson);
			GeometryJSON gjsontool = new GeometryJSON();
			try {
				this.geo = gjsontool.read(input);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//properties
			JsonObject properties = contents.get("properties").getAsJsonObject();
			for(Map.Entry<String, JsonElement> entry :properties.entrySet())
			{
				String key = entry.getKey();
				//System.out.println(key);
				if(entry.getValue().getAsJsonPrimitive().isString())
				{
					this.properties.put(key, new Value().setString_value(entry.getValue().getAsString()));
				}
				else if(entry.getValue().getAsJsonPrimitive().isNumber())
				{
					this.properties.put(key, new Value().setDouble_value(entry.getValue().getAsDouble()));
					//this.properties.put(key, new Value().setDouble_value(Double.parseDouble(entry.getValue().getAsNumber().toString())));
				}
				else if(entry.getValue().getAsJsonPrimitive().isBoolean())
				{
					this.properties.put(key, new Value().setBoolean_value(entry.getValue().getAsBoolean()));
				}
				else
				{
					this.properties.put(key, new Value().setString_value(entry.getValue().getAsString()));
				}
			}
		}
		else
		{
			for(Map.Entry<String, JsonElement> entry :contents.entrySet())
			{
				this.properties = new LinkedHashMap<String,Value>();
				String key = entry.getKey();
				if("id".equalsIgnoreCase(key) || "fid".equalsIgnoreCase(key))
				{
					if(entry.getValue().getAsString() != null)
					{
						this.oid = entry.getValue().getAsString();
					}
					else
					{
						this.oid = String.valueOf(entry.getValue().getAsNumber());
					}
				}
				if("geometry".equalsIgnoreCase(key) || "_geometry_".equalsIgnoreCase(key))
				{
					this.geo_wkt = entry.getValue().getAsString();
				}
				//System.out.println(key);
				if(entry.getValue().getAsJsonPrimitive().isString())
				{
					this.properties.put(key, new Value().setString_value(entry.getValue().getAsString()));
				}
				else if(entry.getValue().getAsJsonPrimitive().isNumber())
				{
					this.properties.put(key, new Value().setDouble_value(entry.getValue().getAsDouble()));
				}
				else if(entry.getValue().getAsJsonPrimitive().isBoolean())
				{
					this.properties.put(key, new Value().setBoolean_value(entry.getValue().getAsBoolean()));
				}
				else
				{
					this.properties.put(key, new Value().setString_value(entry.getValue().getAsString()));
				}
			}
		}
	}

}
