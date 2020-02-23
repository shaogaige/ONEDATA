/**
 * ClassName:TableIndexWriter.java
 * Date:2019年3月29日
 */
package com.idata.core;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.geotools.geojson.GeoJSON;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.lucene.IKAnalyzer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.idata.tool.AESUtil;
import com.idata.tool.FileUtil;
import com.idata.tool.LogUtil;
import com.ojdbc.sql.ConnectionManager;
import com.ojdbc.sql.ConnectionManager.ConnectionInfo;
import com.ojdbc.sql.ConnectionObject;
import com.ojdbc.sql.SQLResultSet;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Creater:SHAO Gaige 
 * Description:索引创建 
 * Log:
 */
public class TableIndexWriter implements Runnable
{
    //数据库连接串
	private String dataBaseURL = "";
    //用户名
	private String userName = "";
    //密码
	private String passWord = "";
	//表名
	private String tableName = null;
	//空间数据字段
	private String geoFiled = null;
	//索引路径
	private String path = "";
	//分词器
	public static Analyzer analyzer;
	//IK
	public static IKSegmenter ikSeg = null;
	//IndexWriter配置
	public static IndexWriterConfig iwc;
	//空间索引
    public static SpatialContext ctx = SpatialContext.GEO;
    
	public static SpatialStrategy strategy;
	
	public static SpatialContextFactory fac = new SpatialContextFactory();
    
	/**
	 * 构造函数
	 * @param constring
	 * @param tableName
	 * @param geoField
	 * @param path
	 */
	public TableIndexWriter(String constring,String tableName,String geoField,String path)
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
		this.geoFiled = geoField;
		this.path = path;
		//使用IK进行分词
		analyzer = new IKAnalyzer(true);
		//加载分词词典
		if(ikSeg == null)
		{
			ikSeg = new IKSegmenter(new StringReader("IK分词"),true);
		}
		System.out.println(ikSeg.toString());
		iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		//空间索引
		int maxLevels = 11;
		SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
		strategy = new RecursivePrefixTreeStrategy(grid, "GEOMETRY_INDEX");
	}

	/**
	 * 创建索引
	 */
	public void createIndex()
	{

		DataBaseHandle databaseHandle = null;
		try 
		{
			databaseHandle = new DataBaseHandle(dataBaseURL, userName, passWord);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

		String geometryField = this.geoFiled;
		if (geometryField == null || "".equalsIgnoreCase(geometryField))
		{
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
			LogUtil.info("开始生成索引.........");
			// 构建地理空间索引
			org.locationtech.spatial4j.io.WKTReader wktReader = new org.locationtech.spatial4j.io.WKTReader(
					ctx, fac);
			WKTReader OGCWKTReader = new WKTReader();
			
			String table = tableName.trim();
			// 解除index reader
			if (OneDataServer.tableReader.containsKey(path))
			{
				String key = OneDataServer.tableOrder.poll();
				IndexReader r = OneDataServer.tableReader.remove(key);
				r.close();
				r = null;
			}
			if (!databaseHandle.isTableExist(table))
			{
				LogUtil.error("Table " + table + "不存在！");
				return;
			}
			else
			{
				LogUtil.info("Table " + table + "正在生成索引...");
			}
			// 删除之前的索引
			try
			{
				File indexFile = new File(path);
				if (indexFile.exists())
				{
					LogUtil.info("正在尝试清除之前的索引.........");
					FileUtil.delFile(indexFile);
				}
			} catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
				LogUtil.error("清除原有索引失败！请查看目录：" + path);
			}
			// 定义索引输出目录
			Directory dir = FSDirectory.open(Paths.get(path));
			// 创建write对象
			IndexWriter writer = new IndexWriter(dir, iwc);
			
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
			while (rs2.next())
			{
				// 创建文档对象
				Document doc = new Document();
				JsonObject feature = new JsonObject();
				// 存入一组数据,键type,值为Feature
				feature.addProperty("type", "Feature");
				JsonObject proper = new JsonObject();

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

								// Shape point = wktReader.parse(wkt);
								Shape shape = null;
								geotype = geometry.getGeometryType();
								//System.out.println("geotype:"+geotype);
								if ("point".equalsIgnoreCase(geotype))
								{
									shape = wktReader.parse(geometry.toText());
								}
								else
								{
									Envelope env = geometry.getEnvelopeInternal();
									shape = new RectangleImpl(env.getMinX(), env.getMaxX(), env.getMinY(),
											env.getMaxY(), ctx);
								}
								for (Field f : strategy.createIndexableFields(shape))
								{
									doc.add(f);
								}
								// 存储信息,用于最后过滤
								Field GEOMETRY_FIELD = new TextField("GEOMETRY_FIELD", geometry.toText(),
										Field.Store.YES);
								doc.add(GEOMETRY_FIELD);
							}
						} catch (Exception e)
						{
							e.printStackTrace();
							LogUtil.error(e);
							continue;
						}
					}
					else
					{
						// 属性字段
						if (type == Types.FLOAT || type == Types.DOUBLE || type == Types.INTEGER
								|| type == Types.SMALLINT || type == Types.NUMERIC || type == Types.DECIMAL
								|| type == Types.REAL)
						{
							double value = rs2.getDouble(columnName);

							proper.addProperty(columnName, value);
							Field field = new DoublePoint(columnName, value);
							doc.add(field);
							Field field2 = new StoredField(columnName, value);
							doc.add(field2);
							SortedDocValuesField sort_field = new SortedDocValuesField(columnName,new BytesRef(String.valueOf(value)));
							doc.add(sort_field);
						}
						else if (type == Types.BOOLEAN || type == Types.BIT)
						{
							boolean value = rs2.getBoolean(columnName);
							proper.addProperty(columnName, value);
							Field field = new StringField(columnName, value + "", Field.Store.YES);
							doc.add(field);
							SortedDocValuesField sort_field = new SortedDocValuesField(columnName,new BytesRef(String.valueOf(value)));
							doc.add(sort_field);
						}
						else
						{
							String value = rs2.getString(columnName);
							if (value != null && !"".equalsIgnoreCase(value))
							{
								value = value.trim();
								Field field = new StringField(columnName,value , Field.Store.YES);
								doc.add(field);
								SortedDocValuesField sort_field = new SortedDocValuesField(columnName,new BytesRef(String.valueOf(value)));
								doc.add(sort_field);
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
				Field ALLCONTENT = null;
				if(this.geoFiled == null || "".equalsIgnoreCase(this.geoFiled))
				{
					ALLCONTENT = new TextField("ALL_JSON_DATA", proper.toString(), Field.Store.YES);
				}
				else
				{
					ALLCONTENT = new TextField("ALL_JSON_DATA", feature.toString(), Field.Store.YES);
				}
				
				// System.out.println(feature.toString());
				doc.add(ALLCONTENT);
				writer.addDocument(doc);
			}
			
			LogUtil.info("Table " + table + "索引生成完毕...");
			writer.close();
			writer = null;
			// System.out.println("reader创建完毕");
			// close resource
			rs2.close();
			stat.close();
			ConnectionManager.returnConnectionObject(conn);
			//创建一个IndexReader对象
			IndexReader indexReader = DirectoryReader.open(dir);
			// 将IndexReader对象存入map池中,将表名存入队列集合
			if (indexReader != null)
			{
				OneDataServer.tableReader.put(path, indexReader);
				OneDataServer.tableOrder.offer(path);
			}
			// 当集合池中元素大于10个,释放并删除头元素
			if (OneDataServer.tableReader.size() > 10)
			{
				String key = OneDataServer.tableOrder.poll();
				IndexReader reader = OneDataServer.tableReader.remove(key);
				reader.close();
				reader = null;
			}
			LogUtil.info("索引生成完毕.........");
		} catch (Exception e)
		{
			e.printStackTrace();
			LogUtil.error(e);
		} finally
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
		createIndex();
	}
}
