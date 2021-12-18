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
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.idata.tool.RandomIDUtil;
import com.ojdbc.sql.Value;
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
	private String properties_s = "";
	//空间坐标数据
	private Geometry geo;
	private String geo_wkt = "";
	
	public String getOid() {
		if(this.oid == null || "".equalsIgnoreCase(this.oid))
		{
			this.oid = RandomIDUtil.getUUID("");
		}
		return oid;
	}
	
	public String getSourceId()
	{
		return oid;
	}
	
	public long getNumID()
	{
		return RandomIDUtil.getNumberID();
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
	public void setGeo(Geometry geo,boolean objectize) {
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
		//刷新字符串格式
		this.properties_s = "";
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
			else if(v.isDoubleValue() || v.isFloatValue())
			{
				feature.addProperty(key, v.getDouble_value());
			}
			else if(v.isIntValue())
			{
				feature.addProperty(key, v.getInt_value());
			}
			else if(v.isLongValue())
			{
				feature.addProperty(key, v.getLong_value());
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
	
	public JsonObject getJSONObject(String out,String geofield)
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
				try 
				{
					if(geo == null)
					{
						WKTReader OGCWKTReader = new WKTReader();
						this.geo = OGCWKTReader.read(geo_wkt);
					}
					GeoJSON.write(geo, output);
				} catch (Exception e) {
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
				if(geofield == null || "".equalsIgnoreCase(geofield))
				{
					geofield = "_geometry_";
				}
				feature.addProperty(geofield, getGeo_wkt());
			}
		}
		return feature;
	}
	
	public String getJSONString(String out,String geofield)
	{
		if(this.properties_s == null || "".equalsIgnoreCase(this.properties_s))
		{
			this.properties_s = getJSONObject(out,geofield).toString();
		}
		return this.properties_s;
	}
	
	public void setProperties(Map<String,Value> properties,String idfield,String geofield)
	{
		if(properties==null) 
		{
			return ;
		}
		if(idfield == null || "".equalsIgnoreCase(idfield))
		{
			idfield = "oid";
		}
		if(geofield == null || "".equalsIgnoreCase(geofield))
		{
			geofield = "_geometry_";
		}
		if(properties.get(idfield) != null)
		{
			this.oid = properties.get(idfield).getString_value();
		}
		if(properties.get(geofield) != null)
		{
			this.geo_wkt = properties.get(geofield).getString_value();
			this.type = "spatial";
		}
		this.properties = properties;
	}
	
	public void setJSONString(String jsons,String idfield,String geofield,boolean objectize)
	{
		if(jsons==null || "".equalsIgnoreCase(jsons)) 
		{
			return ;
		}
		
		this.properties_s = jsons;
		if(!objectize)
		{
			return ;
		}
		
		if(idfield == null || "".equalsIgnoreCase(idfield))
		{
			idfield = "oid";
		}
		if(geofield == null || "".equalsIgnoreCase(geofield))
		{
			geofield = "_geometry_";
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
				if(key.equalsIgnoreCase(idfield))
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
				if(JsonNull.INSTANCE.equals(entry.getValue()))
				{
					continue;
				}
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
				String key = entry.getKey();
				if(key.equalsIgnoreCase(idfield))
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
				
				if(geofield.equalsIgnoreCase(key) || "geometry".equalsIgnoreCase(key))
				{
					this.geo_wkt = entry.getValue().getAsString();
				}
				//System.out.println(key);
				if(JsonNull.INSTANCE.equals(entry.getValue()))
				{
					continue;
				}
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
