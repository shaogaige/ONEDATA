/**
 * ClassName:HbaseImporter.java
 * Date:2020年3月2日
 */
package com.idata.core;

import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.geojson.GeoJSON;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.idata.tool.AESUtil;
import com.idata.tool.GeoHashUtil;
import com.idata.tool.LogUtil;
import com.idata.tool.RandomIDUtil;
import com.ojdbc.sql.ConnectionManager;
import com.ojdbc.sql.ConnectionObject;
import com.ojdbc.sql.SQLResultSet;
import com.ojdbc.sql.ConnectionManager.ConnectionInfo;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Creater:SHAO Gaige
 * Description:Hbase初始化数据导入器
 * Log:
 */
public class HbaseImporter implements Runnable 
{
	//数据库连接串
	private String dataBaseURL = "";
	//用户名
	private String userName = "";
	//密码
	private String passWord = "";
	//表名
	private String tableName = null;
	//id数据字段
	private String idField = null;
	//空间数据字段
	private String geoField = null;
	//Hbase中数据库名称
	private String newTable = null;
	
	
	public HbaseImporter(String constring,String tableName,String idField,String geoField,String newTable)
	{
		String content = constring;
		if(!constring.contains(","))
		{
			content = AESUtil.aesDecrypt(constring, OneDataServer.AESKEY);
		}
		String[] ds = content.split(",");
		if(ds.length == 3)
		{
			dataBaseURL = ds[0];
			userName = ds[1];
			passWord = ds[2];
		}
		else if(ds.length == 1)
		{
			dataBaseURL = ds[0];
		}
		
		this.tableName = tableName;
		this.idField = idField;
		this.geoField = geoField;
		this.newTable = newTable;
	}
	
	
	public void write()
	{
		DataBaseHandle databaseHandle = null;
		try 
		{
			databaseHandle = new DataBaseHandle(dataBaseURL, userName, passWord);
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

		boolean geoflag = true;
		String geometryField = this.geoField;
		if (geometryField == null || "".equalsIgnoreCase(geometryField))
		{
			geoflag = false;
			if(dataBaseURL.contains("oracle"))
			{
				geometryField = "SHAPE";
			}
			else if(dataBaseURL.contains("postgresql"))
			{
				geometryField = "geometry";
			}
		}
		
		
		if(tableName == null || "".equalsIgnoreCase(tableName))
		{
			return;
		}

		try
		{
			LogUtil.info("开始向Hbase导入数据.........");
			
			WKTReader OGCWKTReader = new WKTReader();
			
			String table = tableName.trim();
			
			if (!databaseHandle.isTableExist(table))
			{
				LogUtil.error("Table " + table + "不存在！");
				return;
			}
			else
			{
				LogUtil.info("Table " + table + "正在导入数据...");
			}
			//判断Hbase中表名是否存在
			try
			{
				if (!OneDataServer.hbaseHandle.tableExists(newTable))
				{
					LogUtil.info("Hbase中目标表不存在.........");
					return;
				}
			} 
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// 查询获取表中所有列名
			String sql1 = "SELECT * from " + table;
			SQLResultSet rs1 = databaseHandle.exeSQLSelect(sql1);
			// 遍历拼接查询sql语句
			String sql2 = "select ";
			for (int i = 0; i < rs1.getRowNum(); i++)
			{
				String value = rs1.getRow(i).getValue("COLUMN_NAME").getString_value();
				if (value.equalsIgnoreCase(geometryField))
				{
					if(dataBaseURL.contains("oracle"))
					{
						value = "SDE.st_astext("+geometryField+") as GEOMETRY_IN";
					}
					else if(dataBaseURL.contains("postgresql"))
					{
						value = "ST_AsText("+geometryField+") as GEOMETRY_IN";
					}
					geoflag = true;
				}
				sql2 += value + ",";
				// System.out.println("列名:" + value);
			}
			if (sql2.endsWith(","))
			{
				sql2 = sql2.substring(0, sql2.length() - 1);
			}
			sql2 += " from " + table;
			// 执行查询到表中所有记录
			// System.out.println("sql2:" + sql2);
			LogUtil.info("执行SQL：" + sql2);
			ConnectionInfo connInfo = databaseHandle.getConnectionInfo();
			ConnectionObject conn = ConnectionManager.borrowConnectionObject(connInfo);
			Statement stat = conn.getConnection().createStatement();
			ResultSet rs2 = stat.executeQuery(sql2);
			// 创建可以获取列名的对象
			ResultSetMetaData metaData = rs2.getMetaData();
			// 获取列总数
			int size = metaData.getColumnCount();
			String geotype = "";
			
			boolean idflag = false;
			if(this.idField != null && !"".equalsIgnoreCase(idField))
			{
				idflag = true;
			}
			
			while (rs2.next())
			{
				JsonObject feature = new JsonObject();
				// 存入一组数据,键type,值为Feature
				feature.addProperty("type", "Feature");
				JsonObject proper = new JsonObject();
				
				String id = null;
				if(idflag)
				{
					id = rs2.getString(this.idField);
					if(id == null || "".equalsIgnoreCase(id))
					{
						double id2 = rs2.getDouble(this.idField);
						id = id2+"";
					}
				}
				
				if(geoflag)
				{
					//空间数据
					String geometryValue = rs2.getString("GEOMETRY_IN");
					// 读取坐标信息
					if (geometryValue == null || "".equalsIgnoreCase(geometryValue.trim())
							|| "null".equalsIgnoreCase(geometryValue))
					{
						continue;
					}
					String wkt = geometryValue.trim();
					//修正geometry,主要处理st_astext()函数返回丢失类型
					//System.out.println(wkt.substring(0, 20));
					wkt = correctWKT(wkt,geotype);
					Geometry geometry = OGCWKTReader.read(wkt);
					GeoHashUtil gu = new GeoHashUtil(geometry.getInteriorPoint().getX(),geometry.getInteriorPoint().getY());
					if(id == null || "".equalsIgnoreCase(id))
					{
						
						id = RandomIDUtil.getDate("")+"_"+gu.getGeoHashBase32();
					}
					else
					{
						id += RandomIDUtil.getDate("_")+"_"+gu.getGeoHashBase32();
					}
				}
				else
				{
					//非空间
					if(id == null || "".equalsIgnoreCase(id))
					{
						id = RandomIDUtil.getUUID("");
					}
					else
					{
						id += RandomIDUtil.getUUID("_");
					}
				}
				
				Put p = new Put(id.getBytes());

				for (int i = 1; i <= size; i++)
				{
					String columnName = metaData.getColumnName(i);// 表中所有该索引列的列名
					// String columnValue = rs2.getString(i);// 表中所有该索引列的值
					int type = metaData.getColumnType(i);
					if ("GEOMETRY_IN".equalsIgnoreCase(columnName))
					{
						// 空间字段
						String geometryValue = rs2.getString("GEOMETRY_IN");
						// 读取坐标信息
						if (geometryValue == null || "".equalsIgnoreCase(geometryValue.trim())
								|| "null".equalsIgnoreCase(geometryValue))
						{
							continue;
						}
						try
						{
							String wkt = geometryValue.trim();
							//修正geometry,主要处理st_astext()函数返回丢失类型
							//System.out.println(wkt.substring(0, 20));
							wkt = correctWKT(wkt,geotype);
							Geometry geometry = OGCWKTReader.read(wkt);
							// 投影转换
							//System.out.println(g.toText());
							//System.out.println(geometry.toText());
							
							if (geometry != null)
							{
								StringWriter output = new StringWriter();
								GeoJSON.write(geometry, output);
								// 写入Geometry
								JsonElement geo = new JsonParser().parse(output.toString());
								feature.add("geometry", geo);
								//String geo = output.toString();
								//feature.addProperty("geometry", geo);
								Envelope env = geometry.getEnvelopeInternal();
								
								p.addColumn("geo".getBytes(), "geometry".getBytes(), wkt.getBytes());
								//空间索引
								p.addColumn("geo".getBytes(), "minx".getBytes(), Bytes.toBytes(env.getMinX()));
								p.addColumn("geo".getBytes(), "miny".getBytes(), Bytes.toBytes(env.getMinY()));
								p.addColumn("geo".getBytes(), "maxx".getBytes(), Bytes.toBytes(env.getMaxX()));
								p.addColumn("geo".getBytes(), "maxy".getBytes(), Bytes.toBytes(env.getMaxY()));
							}
						} 
						catch (Exception e)
						{
							e.printStackTrace();
							LogUtil.error(e);
							continue;
						}
					}
					else
					{
						if(columnName.equalsIgnoreCase(this.idField))
						{
							continue;
						}
						// 属性字段
						if (type == Types.FLOAT || type == Types.DOUBLE || type == Types.INTEGER
								|| type == Types.SMALLINT || type == Types.NUMERIC || type == Types.DECIMAL
								|| type == Types.REAL)
						{
							double value = rs2.getDouble(columnName);
							proper.addProperty(columnName, value);
							p.addColumn("data".getBytes(), columnName.getBytes(), Bytes.toBytes(value));
						}
						else if (type == Types.BOOLEAN || type == Types.BIT)
						{
							boolean value = rs2.getBoolean(columnName);
							proper.addProperty(columnName, value);
							p.addColumn("data".getBytes(), columnName.getBytes(), Bytes.toBytes(value));
						}
						else
						{
							String value = rs2.getString(columnName);
							if (value != null && !"".equalsIgnoreCase(value))
							{
								value = value.trim();
								p.addColumn("data".getBytes(), columnName.getBytes(), value.getBytes());
							}
							if (value == null || "".equalsIgnoreCase(value.trim()))
							{
								value = "";
							}
							proper.addProperty(columnName, value);
						}
					}
				}
				feature.add("properties", proper);
				//输出字段的处理
				p.addColumn("out".getBytes(), "all_json_data".getBytes(),feature.toString().getBytes());
				
				OneDataServer.hbaseHandle.put(newTable, p);
			}
			
			LogUtil.info("Table " + table + "数据导入完毕...");

			// System.out.println("reader创建完毕");
			// close resource
			rs2.close();
			stat.close();
			ConnectionManager.returnConnectionObject(conn);
			
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			LogUtil.error(e);
		} 
		finally
		{
			
		}
	}
	
	//矫正geometry的wkt描述
	private String correctWKT(String wkt,String type)
	{
		if(wkt.contains("POINT")||wkt.contains("POLYGON")||wkt.contains("LINESTRING")||
				wkt.contains("MULTILINESTRING")||wkt.contains("MULTIPOLYGON")||wkt.contains("MULTIPOINT"))
		{
			return wkt;
		}
		else
		{
			if(wkt.contains("((("))
			{
				return "MULTIPOLYGON "+wkt;
			}
			else if(wkt.contains("(("))
			{
				if(type.contains("LINESTRING"))
				{
					return "MULTILINESTRING "+wkt;
				}
				return "POLYGON "+wkt;
			}
			else
			{
				if(wkt.contains(","))
				{
					if(type.contains("POINT"))
					{
						return "MULTIPOINT "+wkt; 
					}
					return "LINESTRING "+wkt;
				}
				else
				{
					return "POINT "+wkt;
				}
			}
		}
	}	
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		write();
	}

}
