/**
 * ClassName:HbaseOperator.java
 * Date:2020年3月2日
 */
package com.idata.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import com.idata.tool.GeoHashUtil;
import com.idata.tool.RandomIDUtil;
import com.ojdbc.sql.Value;
import com.vividsolutions.jts.geom.Envelope;

/**
 * Creater:SHAO Gaige
 * Description:Hbase数据访问控制器
 * Log:
 */
public class HbaseOperator {
	
	//构造函数
	public HbaseOperator()
	{
		
	}
	
	public boolean add(DataParam filter)
	{
		Put p = buildPut(filter);
		return OneDataServer.hbaseHandle.put(filter.getLayer(), p);
	}
	
	public boolean edit(DataParam filter)
	{
		//同新增，覆盖
		Put p = buildPut(filter);
		return OneDataServer.hbaseHandle.put(filter.getLayer(), p);
	}
	
	public boolean delete(DataParam filter)
	{
		String idfield = filter.getIdfield();
		if(idfield == null || "".equalsIgnoreCase(idfield))
		{
			idfield = "oid";
		}
		String rowkey = filter.getKeywords();
		Delete d = new Delete(Bytes.toBytes(rowkey));
		return OneDataServer.hbaseHandle.delete(filter.getLayer(), d);
	}
	
	public List<SuperObject> query(DataParam filter)
	{
		Scan s = buildScan(filter);
		return OneDataServer.hbaseHandle.scan(filter.getLayer(),s,filter);
	}
	
	public List<SuperObject> group(DataParam filter)
	{
		Scan sc = buildScan(filter);
		TableName tableName = TableName.valueOf(filter.getLayer());
		try 
		{
			Table t = OneDataServer.hbaseHandle.getConn().getTable(tableName);
			ResultScanner rs = t.getScanner(sc);
			List<SuperObject> so = new ArrayList<SuperObject>();
			boolean f_sum = false;
			if(filter.getSumfield() != null && !"".equalsIgnoreCase(filter.getSumfield()))
			{
				f_sum = true;
			}
			Map<String,Long> group = new HashMap<String,Long>();
			Map<String,Double> sum = new HashMap<String,Double>();
			
			for(Result r:rs)
			{
				for(Cell cell:r.rawCells())
				{
					String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
	                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
	                if(colName.equalsIgnoreCase(filter.getGroupfield()))
	                {
	                	if(group.containsKey(colName))
		                {
		                	group.put(colName,group.get(colName)+1);
		                }
		                else
		                {
		                	group.put(colName,1L);
		                }
	                }
	                if(f_sum)
	                {
	                	if(colName.equalsIgnoreCase(filter.getSumfield()))
	                	{
	                		if(sum.containsKey(colName))
			                {
	                			sum.put(colName,sum.get(colName)+Double.valueOf(value));
			                }
			                else
			                {
			                	sum.put(colName,Double.valueOf(value));
			                }
	                	}
	                }
				}
			}
			
			for(String key:group.keySet())
			{
				SuperObject s = new SuperObject();
				s.setJSONString("[\""+key+"\","+group.get(key)+","+sum.get(key)+"]", null, null, false);
				so.add(s);
			}
			
			t.close();
			return so;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	
	private Scan buildScan(DataParam filter)
	{
		//get scan
		if(filter.getIdfield() != null && filter.getIdfield().equalsIgnoreCase(filter.getQueryfields()))
		{
			Get get = new Get(filter.getKeywords().getBytes());
			Scan sc = new Scan(get);
			return sc;
		}
		
		Scan sc = new Scan();
		FilterList filterList = null;
		if("query".equalsIgnoreCase(filter.getOperation()))
		{
			if(filter.getOutFields() == null || "".equalsIgnoreCase(filter.getOutFields()))
			{
				//全部输出
				sc.addColumn("out".getBytes(), "all_json_data".getBytes());
			}
			else
			{
				//指定输出
				String[] fileds = filter.getOutFields().split(",");
				for(int i=0;i<fileds.length;i++)
				{
					if(filter.getGeofield() != null && filter.getGeofield().equalsIgnoreCase(fileds[i]))
					{
						sc.addColumn("geo".getBytes(), fileds[i].getBytes());
					}
					else
					{
						sc.addColumn("data".getBytes(), fileds[i].getBytes());
					}
				}
			}
		}
		else
		{
			//group
			sc.addColumn("data".getBytes(), filter.getGroupfield().getBytes());
			if(filter.getSumfield() != null && !"".equalsIgnoreCase(filter.getSumfield()))
			{
				sc.addColumn("data".getBytes(), filter.getSumfield().getBytes());
			}
		}
		
		
		if(filter.getQueryfields() != null && !"".equalsIgnoreCase(filter.getQueryfields()))
		{
			String[] qfields = filter.getQueryfields().split(",");
			String[] qopers = filter.getQueryoperates().split(",");
			String[] keywords = filter.getKeywords().split(",");
			//String[] qrelas = filter.getQueryrelations().split(",");
			
			if("or".equalsIgnoreCase(filter.getQueryrelations()) || filter.getQueryrelations().contains("or"))
			{
				filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			}
			else
			{
				filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			}
			
			for(int i=0;i<qfields.length;i++)
			{
				SingleColumnValueFilter f = null;
				if("=".equalsIgnoreCase(qopers[i]))
				{
					f = new SingleColumnValueFilter(Bytes.toBytes("data"),Bytes.toBytes(qfields[i]),
							CompareOp.EQUAL,Bytes.toBytes(keywords[i]));
					f.setFilterIfMissing(true);
				}
				else if(">".equalsIgnoreCase(qopers[i]))
				{
					f = new SingleColumnValueFilter(Bytes.toBytes("data"),Bytes.toBytes(qfields[i]),
							CompareOp.GREATER,Bytes.toBytes(keywords[i]));
					f.setFilterIfMissing(true);
				}
				else if("<".equalsIgnoreCase(qopers[i]))
				{
					f = new SingleColumnValueFilter(Bytes.toBytes("data"),Bytes.toBytes(qfields[i]),
							CompareOp.LESS,Bytes.toBytes(keywords[i]));
					f.setFilterIfMissing(true);
				}
				else if(">=".equalsIgnoreCase(qopers[i]))
				{
					f = new SingleColumnValueFilter(Bytes.toBytes("data"),Bytes.toBytes(qfields[i]),
							CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(keywords[i]));
					f.setFilterIfMissing(true);
				}
				else if("<=".equalsIgnoreCase(qopers[i]))
				{
					f = new SingleColumnValueFilter(Bytes.toBytes("data"),Bytes.toBytes(qfields[i]),
							CompareOp.LESS_OR_EQUAL,Bytes.toBytes(keywords[i]));
					f.setFilterIfMissing(true);
				}
				else if("like".equalsIgnoreCase(qopers[i]))
				{
					SubstringComparator comp = new SubstringComparator(keywords[i]);
					f = new SingleColumnValueFilter(Bytes.toBytes("data"),Bytes.toBytes(qfields[i]),
							CompareOp.EQUAL,comp);
					f.setFilterIfMissing(true);
				}
				else if("!=".equalsIgnoreCase(qopers[i]))
				{
					f = new SingleColumnValueFilter(Bytes.toBytes("data"),Bytes.toBytes(qfields[i]),
							CompareOp.NOT_EQUAL,Bytes.toBytes(keywords[i]));
					f.setFilterIfMissing(true);
				}
				else
				{
					f = new SingleColumnValueFilter(Bytes.toBytes("data"),Bytes.toBytes(qfields[i]),
							CompareOp.EQUAL,Bytes.toBytes(keywords[i]));
					f.setFilterIfMissing(true);
				}
				
				filterList.addFilter(f);
			}
		}
		
		if(filter.getBbox() != null && !"".equalsIgnoreCase(filter.getBbox()))
		{
			sc.addColumn("geo".getBytes(), filter.getGeofield().getBytes());
			//空间数据查询
			Envelope box = filter.getFilterGeometry().getEnvelopeInternal();
			SingleColumnValueFilter f_minx = new SingleColumnValueFilter(Bytes.toBytes("geo"),Bytes.toBytes("minx"),
					CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(box.getMinX()));
			f_minx.setFilterIfMissing(true);
			filterList.addFilter(f_minx);
			
			SingleColumnValueFilter f_miny = new SingleColumnValueFilter(Bytes.toBytes("geo"),Bytes.toBytes("miny"),
					CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(box.getMinY()));
			f_miny.setFilterIfMissing(true);
			filterList.addFilter(f_miny);
			
			SingleColumnValueFilter f_maxx = new SingleColumnValueFilter(Bytes.toBytes("geo"),Bytes.toBytes("maxx"),
					CompareOp.LESS_OR_EQUAL,Bytes.toBytes(box.getMaxX()));
			f_maxx.setFilterIfMissing(true);
			filterList.addFilter(f_maxx);
			
			SingleColumnValueFilter f_maxy = new SingleColumnValueFilter(Bytes.toBytes("geo"),Bytes.toBytes("maxy"),
					CompareOp.LESS_OR_EQUAL,Bytes.toBytes(box.getMaxY()));
			f_maxy.setFilterIfMissing(true);
			filterList.addFilter(f_maxy);
		}
		
		sc.setFilter(filterList);
		
		return sc;
	}
	
	private Put buildPut(DataParam filter)
	{
		SuperObject data = new SuperObject();
		data.setJSONString(filter.getJsondata(), filter.getIdfield(), filter.getGeofield(),true);
		//行键的处理
		String id = data.getSourceId();
		if("normal".equalsIgnoreCase(data.getType()))
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
		else
		{
			//空间数据
			GeoHashUtil gu = new GeoHashUtil(data.getGeo().getInteriorPoint().getX(),data.getGeo().getInteriorPoint().getY());
			if(id == null || "".equalsIgnoreCase(id))
			{
				
				id = RandomIDUtil.getDate("")+"_"+gu.getGeoHashBase32();
			}
			else
			{
				id += RandomIDUtil.getDate("_")+"_"+gu.getGeoHashBase32();
			}
		}
		//id行键
		String idfield = filter.getIdfield();
		if(idfield == null || "".equalsIgnoreCase(idfield))
		{
			idfield = "oid";
		}
		data.addProperty(idfield, new Value().setString_value(id));
		
		Put p = new Put(id.getBytes());
		//属性信息的处理
		for(String key:data.getKeys())
		{
			Value v = data.getProperty(key);
			// 属性字段
			if(v.isStringValue())
			{
				p.addColumn("data".getBytes(), key.getBytes(), v.getString_value().getBytes());
			}
			else if(v.isDoubleValue())
			{
				p.addColumn("data".getBytes(),key.getBytes(), Bytes.toBytes(v.getDouble_value()));
			}
			else if(v.isIntValue())
			{
				p.addColumn("data".getBytes(),key.getBytes(), Bytes.toBytes(v.getInt_value()));
			}
			else if(v.isLongValue())
			{
				p.addColumn("data".getBytes(),key.getBytes(), Bytes.toBytes(v.getLong_value()));
			}
			else if(v.isBooleanValue())
			{
				p.addColumn("data".getBytes(),key.getBytes(), Bytes.toBytes(v.getBoolean_value()));
			}
			else
			{
				p.addColumn("data".getBytes(),key.getBytes(), Bytes.toBytes(v.getString_value()));
			}
		}
		//输出字段的处理
		p.addColumn("out".getBytes(), "all_json_data".getBytes(),data.getJSONString("geojson", filter.getGeofield()).getBytes());
		//空间字段的处理
		if("spatial".equalsIgnoreCase(data.getType()))
		{
			p.addColumn("geo".getBytes(), "geometry".getBytes(), data.getGeo_wkt().getBytes());
			//空间索引
			p.addColumn("geo".getBytes(), "minx".getBytes(), Bytes.toBytes(data.getGeo().getEnvelopeInternal().getMinX()));
			p.addColumn("geo".getBytes(), "miny".getBytes(), Bytes.toBytes(data.getGeo().getEnvelopeInternal().getMinY()));
			p.addColumn("geo".getBytes(), "maxx".getBytes(), Bytes.toBytes(data.getGeo().getEnvelopeInternal().getMaxX()));
			p.addColumn("geo".getBytes(), "maxy".getBytes(), Bytes.toBytes(data.getGeo().getEnvelopeInternal().getMaxY()));
		}
		return p;
	}

}
