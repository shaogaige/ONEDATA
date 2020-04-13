/**
 * ClassName:HbaseHandle.java
 * Date:2020年3月5日
 */
package com.idata.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

import com.idata.data.IDataDriver;
import com.ojdbc.sql.Value;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Creater:SHAO Gaige
 * Description:Hbase操作工具类
 * Log:
 */
public class HbaseHandle {
	
	//hbase配置
	private Configuration configuration = null;
	//hbase连接
	private Connection connection = null;
	//hbase管理对象
	private Admin admin = null;
	
	public HbaseHandle(Configuration configuration)
	{
		this.configuration = configuration;
		if(this.configuration != null)
		{
			 try
			 {
				 connection = ConnectionFactory.createConnection(configuration);
			     admin = connection.getAdmin();
			 }
			 catch (IOException e) 
			 {
			     e.printStackTrace();
			 }
		}
	}
	/**
	 * 获取Hbase数据库连接
	 * @return Connection
	 */
	public Connection getConn()
	{
		if(this.connection == null)
		{
			 try
			 {
				 this.connection = ConnectionFactory.createConnection(configuration);
			 }
			 catch (IOException e) 
			 {
			     e.printStackTrace();
			 }
		}
		return this.connection;
	}
	/**
	 * 获取Hbase数据库管理对象
	 * @return Admin
	 */
	public Admin getAdmin()
	{
		if(this.admin == null)
		{
			if(this.connection == null)
			{
				this.connection = getConn();
			}
			try 
			{
				this.admin = connection.getAdmin();
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return this.admin;
	}
	
	/**
	 * list all tables
	 */
	public List<String> listTables()
	{
	    try 
	    {
	        //获取所有的表名
	        TableName [] tableNames = admin.listTableNames();
	        List<String> tables = new ArrayList<String>();
	        if (tableNames.length == 0)
	        {
	        	System.out.println("HBase has no table");
	        }
	        else 
	        {
	            for (TableName tableName:tableNames)
	            {
	            	tables.add(tableName.toString());
	                //System.out.println("tableName:"+tableName.toString());
	            }
	        }
	        return tables;
	    } 
	    catch (IOException e) 
	    {
	        e.printStackTrace();
	        return null;
	    }
	}
	/**
	 * 判断表是否存在
	 * @param tname
	 * @return boolean
	 */
	public boolean tableExists(String tname)
	{
		TableName tableName = TableName.valueOf(tname);
		try 
		{
			return this.admin.tableExists(tableName);
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * create table
	 * tname:table name
	 * return:Boolean
	 */
	public boolean createTable(String tname,String... cfamily)
	{
	    //To String convert to TableName
	    TableName tableName = TableName.valueOf(tname);
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        boolean flag = admin.tableExists(tableName);
	        if (flag) 
	        {
	        	System.out.println("Table has existed");
	            return !flag;
	        }
	        else 
	        {
	            //判断当前的表是否被禁用了,是就开启
	            /*if (admin.isTableDisabled(tableName)){
	                admin.enableTable(tableName);
	            }*/
	            HTableDescriptor hbaseTable = new HTableDescriptor(tableName);
	
	            //通过可变参数来传递不定量的列簇
	            for (String cf:cfamily)
	            {
	                hbaseTable.addFamily(new HColumnDescriptor(cf));
	            }
	            admin.createTable(hbaseTable);
	            return true;
	        }
	    }
	    catch (IOException e) 
	    {
	        e.printStackTrace();
	        return false;
	    }
	}
	/**
	 * 修改表结构
	 * @param tname
	 * @param tableDescriptor
	 * @return boolean
	 */
	public boolean modifyTable(String tname,HTableDescriptor tableDescriptor)
	{
		TableName tableName = TableName.valueOf(tname);
		try 
		{
			this.admin.modifyTable(tableName, tableDescriptor);
			return true;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * delete table
	 */
	public boolean deleteTable(String tname)
	{
	    //To String convert to TableName
	    TableName tableName = TableName.valueOf(tname);
	    try
	    {
	        //判断tname是否存在,存在就返回true,否则返回false
	    	boolean flag = admin.tableExists(tableName);
	        if (flag) 
	        {
	            //禁用表
	            admin.disableTable(tableName);
	            //删除表
	            admin.deleteTable(tableName);
	            return true;
	        }
	        else 
	        {
	        	System.out.println("Table is not exist");
	            return flag;
	        }
	    }
	    catch (IOException e) 
	    {
	        e.printStackTrace();
	        return false;
	    }
	}
	/**
	 * enableTable
	 * @param tname
	 * @return boolean
	 */
	public boolean enableTable(String tname)
	{
		TableName tableName = TableName.valueOf(tname);
		try 
		{
			this.admin.enableTable(tableName);
			return true;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * disableTable
	 * @param tname
	 * @return boolean
	 */
	public boolean disableTable(String tname)
	{
		TableName tableName = TableName.valueOf(tname);
		try 
		{
			this.admin.disableTable(tableName);
			return true;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	/**
	 * 执行put操作
	 * @param tname
	 * @param p
	 * @return
	 */
	public boolean put(String tname,Put p)
	{
		TableName tableName = TableName.valueOf(tname);
		try 
		{
			Table t = this.connection.getTable(tableName);
			t.put(p);
			t.close();
			return true;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	}
	/**
	 * 执行批量put操作
	 * @param tname
	 * @param puts
	 * @return boolean
	 */
	public boolean batchPuts(String tname,List<Put> puts)
	{
		TableName tableName = TableName.valueOf(tname);
		try 
		{
			Table t = this.connection.getTable(tableName);
			t.put(puts);
			t.close();
			return true;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	/**
	 * 执行Get操作
	 * @param tname
	 * @param g
	 * @return SuperObject
	 */
	public SuperObject get(String tname,Get g)
	{
		TableName tableName = TableName.valueOf(tname);
		try 
		{
			Table t = this.connection.getTable(tableName);
			Result r = t.get(g);
			SuperObject s = new SuperObject();
			for(Cell cell:r.rawCells())
			{
				String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                s.addProperty(colName, new Value().setString_value(value));
			}
			t.close();
			return s;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * 执行Delete操作
	 * @param tname
	 * @param d
	 * @return boolean
	 */
	public boolean delete(String tname,Delete d)
	{
		TableName tableName = TableName.valueOf(tname);
		try 
		{
			Table t = this.connection.getTable(tableName);
			t.delete(d);
			t.close();
			return true;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	/**
	 * 执行scan操作
	 * @param tname
	 * @param scan
	 * @return List<SuperObject>
	 */
	public List<SuperObject> scan(String tname,Scan scan,DataParam filter)
	{
		TableName tableName = TableName.valueOf(tname);
		try
		{
			//添加Hbase协处理器
			admin.disableTable(tableName);
			HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
	        String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
	        if (! descriptor.hasCoprocessor(coprocessorClass)) {
	            descriptor.addCoprocessor(coprocessorClass);
	        }
	        admin.modifyTable(tableName, descriptor);
	        admin.enableTable(tableName);
	        //end
			Table t = this.connection.getTable(tableName);
			AggregationClient aggregationClient = new AggregationClient(configuration);
			long size = 0;
			try 
			{
				size = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
				aggregationClient.close();
			} catch (Throwable e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				aggregationClient.close();
			}
			
			//查询
			ResultScanner rs = t.getScanner(scan);
			List<SuperObject> so = new ArrayList<SuperObject>();
			boolean flag = false;
			WKTReader OGCWKTReader = null;
			if(filter.getBbox() != null && !"".equalsIgnoreCase(filter.getBbox()))
			{
				flag = true;
				OGCWKTReader = new WKTReader();
			}
			//支持分页，过滤
			int start = 0;
			if(filter.getStart()>0 && filter.getStart()<=size) 
			{
				start = filter.getStart();
			}
			
			int count = (int) size;
			if(filter.getCount()>0 && ((filter.getCount()+start)<size))
			{
				count = filter.getCount()+start;
			}
			int k = 0;
			long resultsize = size;
			
			for(Result r:rs)
			{
				if(k >= start)
				{
					if(k<count)
					{
						SuperObject s = new SuperObject();
						for(Cell cell:r.rawCells())
						{
							String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
			                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
			                
			                if("all_json_data".equalsIgnoreCase(colName))
			                {
			                	if(flag)
			                	{
			                		PreparedGeometry mainGeo = filter.getFilterPreparedGeometry();
			                		String spatialRelation = filter.getGeoaction();
			                		if(filter.getGeofield().equalsIgnoreCase(colName))
				                	{
			                			try 
			                			{
											Geometry geo = OGCWKTReader.read(value);
											if("Intersects".equalsIgnoreCase(spatialRelation))
											{
												if(!mainGeo.intersects(geo))
												{
													resultsize--;
													break;
												}
											}
											else if("Contains".equalsIgnoreCase(spatialRelation))
											{
												if(!mainGeo.contains(geo))
												{
													resultsize--;
													break;
												}
											}
											else if("Within".equalsIgnoreCase(spatialRelation))
											{
												if(!mainGeo.within(geo))
												{
													resultsize--;
													break;
												}
											}
										} 
			                			catch (ParseException e) 
			                			{
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
				                	}
			                	}
			                	else
			                	{
			                		s.setJSONString(value, null, null, false);
			                	}
			                	
			                }
			                else
			                {
			                	if(flag)
			                	{
			                		PreparedGeometry mainGeo = filter.getFilterPreparedGeometry();
			                		String spatialRelation = filter.getGeoaction();
			                		if(filter.getGeofield().equalsIgnoreCase(colName))
				                	{
			                			try 
			                			{
											Geometry geo = OGCWKTReader.read(value);
											if("Intersects".equalsIgnoreCase(spatialRelation))
											{
												if(!mainGeo.intersects(geo))
												{
													resultsize--;
													break;
												}
											}
											else if("Contains".equalsIgnoreCase(spatialRelation))
											{
												if(!mainGeo.contains(geo))
												{
													resultsize--;
													break;
												}
											}
											else if("Within".equalsIgnoreCase(spatialRelation))
											{
												if(!mainGeo.within(geo))
												{
													resultsize--;
													break;
												}
											}
										} 
			                			catch (ParseException e) 
			                			{
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
				                	}
			                	}
			                	else
			                	{
			                		if(filter.getGeofield() != null && filter.getGeofield().equalsIgnoreCase(colName))
				                	{
				                		s.setGeo_wkt(value);
				                	}
				                	else
				                	{
				                		s.addProperty(colName, new Value().setString_value(value));
				                	}
			                	}
			                }
			                
						}
						k++;
						so.add(s);
					}
					else
					{
						break;
					}
				}
				
			}
			IDataDriver.resultSize.put(filter.toString(), resultsize);
			rs.close();
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
	
	
	
}
