/**
 * ClassName:DataBaseDriver.java 
 * Date:2020年1月19日
 */
package com.idata.data;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.idata.core.DataBaseHandle;
import com.idata.core.DataParam;
import com.idata.core.OneDataServer;
import com.idata.core.SuperObject;
import com.idata.cotrol.VisitControl;
import com.idata.tool.LogUtil;
import com.idata.tool.RandomIDUtil;
import com.ojdbc.sql.PreparedParam;
import com.ojdbc.sql.SQLResultSet;
import com.ojdbc.sql.Value;

/**
 * Creater:SHAO Gaige
 * Description:数据库类型驱动类
 * Log:
 */
public class DataBaseDriver implements IDataDriver {
	
	private DataBaseHandle DBHandle = null;
	
	public void setDataBaseHandle(DataBaseHandle handle)
	{
		this.DBHandle = handle;
	}
	
	//获取数据库连接类
	public DataBaseHandle getDBHandle(DataParam param)
	{
		if(DBHandle != null)
		{
			return DBHandle;
		}
		else if(param.getCon() != null && !"".equalsIgnoreCase(param.getCon()))
		{
			try 
			{
				DBHandle = new DataBaseHandle(param.getCon());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				LogUtil.error(e);
				VisitControl.log(param, e, LogUtil.Level.ERROR, "Data");
				return null;
			}
		}
		else if(param.getUid() != null && !"".equalsIgnoreCase(param.getUid()))
		{
			String con = null;
			if(OneDataServer.databaseinfo.containsKey(param.getUid()))
			{
				con = OneDataServer.databaseinfo.getObject(param.getUid()).getProperty("conencode").getString_value();
			}
			else
			{
				//使用ID获取数据库连接
				String sql = "select * from "+OneDataServer.TablePrefix+"databaseinfo where id='"+param.getUid()+"'";
				List<SuperObject> rs = OneDataServer.SystemDBHandle.exeSQLSelect2(sql, 1, 1, "id", null);
				if(rs != null)
				{
					con = rs.get(0).getProperty("conencode").getString_value();
					OneDataServer.databaseinfo.add(param.getUid(), rs.get(0));
				}
			}
			try 
			{
				DBHandle = new DataBaseHandle(con);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				LogUtil.error(e);
				VisitControl.log(param, e, LogUtil.Level.ERROR, "Data");
				return null;
			}
		}
		return DBHandle;
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#add(com.idata.core.DataParam)
	 */
	@Override
	public boolean add(DataParam param) {
		// TODO Auto-generated method stub
		try 
		{
			//数据库连接处理
			DBHandle = getDBHandle(param);
			SuperObject data = new SuperObject();
			data.setJSONString(param.getJsondata(), param.getIdfield(), param.getGeofield(),true);
			PreparedParam preparedParam = new PreparedParam();
			String sql = addprocess(param,data,preparedParam);
			if(!"onedata_visit".equalsIgnoreCase(param.getLayer()))
			{
				System.out.println("exeAddSQL:"+sql);
			}
			int f = DBHandle.exePreparedSQLInsert(sql, preparedParam);
			if(f>0)
			{
				return true;
			}
			else
			{
				return false;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LogUtil.error(e);
			VisitControl.log(param, e, LogUtil.Level.ERROR, "Data");
			return false;
		}
	}
	
	//新增内部处理流程
	private String addprocess(DataParam param,SuperObject data,PreparedParam preparedParam)
	{
		List<SuperObject> rsd = this.getMeta(param);
		int size = rsd.size();
		String sql = "insert into "+param.getLayer()+ "(";
		String values = "(";
		int index = 1;
		//获取所有列名称
		for(int i=0;i<size;i++)
		{
			String columnName = rsd.get(i).getProperty("name").getString_value();
			if(columnName.equalsIgnoreCase(param.getIdfield()))
			{
				//id
				if(data.getProperty(columnName) == null)
				{
					continue;
				}
				int type = rsd.get(i).getProperty("type").getInt_value();
				if(type==Types.VARCHAR||type==Types.LONGVARCHAR||type==Types.NCHAR||
						type==Types.NVARCHAR||type==Types.LONGNVARCHAR)
				{
					int p = rsd.get(i).getProperty("precision").getInt_value();
					String id = new StringBuffer(RandomIDUtil.getUUID("")).reverse().toString();
					int max = p>id.length()?id.length():p;
					id = id.substring(0, max);
					id = new StringBuffer(id).reverse().toString();
					sql += columnName+",";
					values += "?,";
					preparedParam.addParam(index, new Value().setString_value(id));
				}
				else
				{
					long numid = data.getNumID();
					String maxidsql = "select max("+param.getIdfield()+") as max_id from "+param.getLayer();
					SQLResultSet r = DBHandle.exeSQLSelect(maxidsql);
					if(r != null && r.getRowNum()>0)
					{
						numid = r.getRow(0).getValue("max_id").getLong_value();
					}
					sql += columnName+",";
					values += "?,";
					preparedParam.addParam(index, new Value().setLong_value(numid+1));
				}
			}
			else
			{
				Value v = data.getProperty(columnName);
				if(v == null)
				{
					continue;
				}
				//空间字段的处理
				if(columnName.equalsIgnoreCase(param.getGeofield()))
				{
					sql += columnName+",";
					if(DBHandle.getConnectionString().contains("oracle"))
					{
						values += "sde.st_geometry(?,4326),";
					}
					else if(DBHandle.getConnectionString().contains("postgresql"))
					{
						values += "ST_GeomFromText(?, 4490),";
					}
				}
				else
				{
					sql += columnName+",";
					values += "?,";
				}
				preparedParam.addParam(index, v);
				
			}
			index++;
		}
		sql = sql.substring(0, sql.length()-1)+") values";
		values = values.substring(0, values.length()-1)+")";
		sql = sql+values;
		return sql;
	}
	
	/**
	 * 批量新增
	 */
	@Override
	public boolean adds(DataParam param) {
		// TODO Auto-generated method stub
		try 
		{
			//数据库连接处理
			DBHandle = getDBHandle(param);
			JsonArray datas = param.getJsonArray();
			String sql = "";
			List<PreparedParam> psqls = new ArrayList<PreparedParam>();
			int size = datas.size();
			if(size == 0)
			{
				return false;
			}
			for(int i=0;i<size;i++)
			{
				SuperObject data = new SuperObject();
				data.setJSONString(datas.get(i).getAsJsonObject().toString(), param.getIdfield(), param.getGeofield(),true);
				PreparedParam preparedParam = new PreparedParam();
				sql = addprocess(param,data,preparedParam);
				
				psqls.add(preparedParam);
			}
			LogUtil.debug("exeTransactionPreparedSQL:"+sql);
			return DBHandle.exePreparedBatchSQL(sql,psqls);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LogUtil.error(e);
			VisitControl.log(param, e, LogUtil.Level.ERROR, "Data");
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#edit(com.idata.core.DataParam)
	 */
	@Override
	public boolean edit(DataParam param) {
		// TODO Auto-generated method stub
		try 
		{
			//数据库连接处理
			DBHandle = getDBHandle(param);
			SuperObject data = new SuperObject();
			data.setJSONString(param.getJsondata(), param.getIdfield(), param.getGeofield(),true);
			List<SuperObject> rsd = this.getMeta(param);
			int size = rsd.size();
			String sql = "update "+param.getLayer()+" set ";
			PreparedParam preparedParam = new PreparedParam();
			int cur = 1;
			//获取所有列名称,应该按上传的字段
			for(int i=0;i<size;i++)
			{
				String columnName = rsd.get(i).getProperty("name").getString_value();
				Value v = data.getProperty(columnName);
				if(v == null)
				{
					continue;
				}
				//空间字段的处理
				if(columnName.equalsIgnoreCase(param.getGeofield()))
				{
					sql += columnName+"";
					if(DBHandle.getConnectionString().contains("oracle"))
					{
						sql += "=sde.st_geometry(?,4326),";
					}
					else if(DBHandle.getConnectionString().contains("postgresql"))
					{
						sql += "=ST_GeomFromText(?, 4490),";
					}
				}
				else
				{
					sql += columnName+"=?,";
				}
				
				preparedParam.addParam(cur, data.getProperty(columnName));
				cur++;
			}
			sql = sql.substring(0, sql.length()-1);
			
			if(param.getQueryfields() != null && !"".equalsIgnoreCase(param.getQueryfields()))
			{
				if(!sql.contains(" where "))
				{
					sql += " where 1=1";
				}
				
				String[] qfields = param.getQueryfields().split(",");
				String[] qopers = param.getQueryoperates().split(",");
				String[] keywords = param.getQueryvalues().split(",");
				String[] qrelas = param.getQueryrelations().split(",");
				for(int i=0;i<qfields.length;i++)
				{
					String qoper = null;
					if((i>qopers.length-1) || "".equalsIgnoreCase(qopers[i]))
					{
						qoper = "=";
					}
					else
					{
						qoper = qopers[i];
					}
					String qrela = null;
					if((i>qrelas.length-1) || "".equalsIgnoreCase(qrelas[i]))
					{
						qrela = "and";
					}
					else
					{
						qrela = qrelas[i];
					}
					
					sql += " "+qrela+" "+qfields[i]+" ";
					if(!"like".equalsIgnoreCase(qoper))
					{
						if("=".equalsIgnoreCase(qoper) || "!=".equalsIgnoreCase(qoper))
						{
							sql += qoper+"'"+keywords[i]+"'";
						}
						else
						{
							sql += qoper+keywords[i];
						}
					}
					else
					{
						sql += qoper+" '%"+keywords[i]+"%'";
					}
				}
			}
			
			if(param.getUsersql()!= null && !"".equalsIgnoreCase(param.getUsersql()))
			{
				if(!sql.contains(" where "))
				{
					sql += " where 1=1";
				}
				sql += " and "+param.getUsersql();
			}
			if("onedata_datavisit".equalsIgnoreCase(param.getLayer()))
			{
				//控制台输出
				//System.out.println("exeEditSQL:"+sql);
			}
			else
			{
				LogUtil.debug("exeEditSQL:"+sql);
			}
			boolean f = DBHandle.exePreparedSQLUpdate(sql, preparedParam);
			return f;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LogUtil.error(e);
			VisitControl.log(param, e, LogUtil.Level.ERROR, "Data");
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#delete(com.idata.core.DataParam)
	 */
	@Override
	public boolean delete(DataParam param) {
		// TODO Auto-generated method stub
		try
		{
			//数据库连接处理
			DBHandle = getDBHandle(param);
			String sql = "delete from "+param.getLayer();
			if(param.getQueryfields() != null && !"".equalsIgnoreCase(param.getQueryfields()))
			{
				if(!sql.contains(" where "))
				{
					sql += " where 1=1";
				}
				
				String[] qfields = param.getQueryfields().split(",");
				String[] qopers = param.getQueryoperates().split(",");
				String[] keywords = param.getQueryvalues().split(",");
				String[] qrelas = param.getQueryrelations().split(",");
				for(int i=0;i<qfields.length;i++)
				{
					String qoper = null;
					if((i>qopers.length-1) || "".equalsIgnoreCase(qopers[i]))
					{
						qoper = "=";
					}
					else
					{
						qoper = qopers[i];
					}
					String qrela = null;
					if((i>qrelas.length-1) || "".equalsIgnoreCase(qrelas[i]))
					{
						qrela = "and";
					}
					else
					{
						qrela = qrelas[i];
					}
					
					sql += " "+qrela+" "+qfields[i];
					if(!"like".equalsIgnoreCase(qoper))
					{
						if("=".equalsIgnoreCase(qoper) || "!=".equalsIgnoreCase(qoper))
						{
							sql += qoper+"'"+keywords[i]+"'";
						}
						else
						{
							sql += qoper+keywords[i];
						}
					}
					else
					{
						sql += qoper+"'%"+keywords[i]+"%'";
					}
				}
				LogUtil.debug("exeDeleteSQL:"+sql);
				return DBHandle.exeSQLDelete(sql);
			}
			else
			{
				return false;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LogUtil.error(e);
			VisitControl.log(param, e, LogUtil.Level.ERROR, "Data");
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#query(com.idata.core.DataParam)
	 */
	@Override
	public List<SuperObject> query(DataParam param) {
		// TODO Auto-generated method stub
		try
		{
			//数据库连接处理
			//使用id查询的处理
			DBHandle = getDBHandle(param);
			String sql = null;
			//查询参数的处理
			if(param.getOutfields() == null || "".equalsIgnoreCase(param.getOutfields()))
			{
				sql = "select * from "+param.getLayer();
				if(param.getGeofield() != null && !"".equalsIgnoreCase(param.getGeofield()))
				{
					List<SuperObject> rsd = this.getMeta(param);
					sql = "select ";
					for (int i = 0; i < rsd.size(); i++)
					{
						String field = rsd.get(i).getProperty("name").getString_value();
						//geometry字段的处理
						if (param.getGeofield().equalsIgnoreCase(field))
						{
							if(DBHandle.getConnectionString().contains("oracle"))
							{
								field = "sde.st_astext("+param.getGeofield()+") as "+param.getGeofield();
							}
							else if(DBHandle.getConnectionString().contains("postgresql"))
							{
								field = "ST_AsText("+param.getGeofield()+") as "+param.getGeofield();
							}
						}
						sql += field + ",";
					}
					if (sql.endsWith(","))
					{
						sql = sql.substring(0, sql.length() - 1);
					}
					sql += " from " + param.getLayer();
				}
			}
			else
			{
				//geometry字段的处理
				sql = "select "+param.getOutfields()+" from "+param.getLayer();
				if(param.getGeofield() != null && !"".equalsIgnoreCase(param.getGeofield()))
				{
					if(param.getOutfields().contains(param.getGeofield()))
					{
						String str = "";
						if(DBHandle.getConnectionString().contains("oracle"))
						{
							str = "sde.st_astext("+param.getGeofield()+") as "+param.getGeofield();
						}
						else if(DBHandle.getConnectionString().contains("postgresql"))
						{
							str = "ST_AsText("+param.getGeofield()+") as "+param.getGeofield();
						}
						
						sql.replace(param.getGeofield(), str);
					}
				}
			}
			
			if(param.getQueryfields() != null && !"".equalsIgnoreCase(param.getQueryfields()))
			{
				if(!sql.contains(" where "))
				{
					sql += " where 1=1";
				}
				
				String[] qfields = param.getQueryfields().split(",");
				String[] qopers = param.getQueryoperates().split(",");
				String[] qvalues = param.getQueryvalues().split(",");
				String[] qrelas = param.getQueryrelations().split(",");
				for(int i=0;i<qfields.length;i++)
				{
					String qoper = null;
					if((i>qopers.length-1) || "".equalsIgnoreCase(qopers[i]))
					{
						qoper = "=";
					}
					else
					{
						qoper = qopers[i];
					}
					String qrela = null;
					if((i>qrelas.length-1) || "".equalsIgnoreCase(qrelas[i]))
					{
						qrela = "and";
					}
					else
					{
						qrela = qrelas[i];
					}
					
					sql += " "+qrela+" "+qfields[i]+" ";
					if(!"like".equalsIgnoreCase(qoper))
					{
						if("=".equalsIgnoreCase(qoper) || "!=".equalsIgnoreCase(qoper))
						{
							if(qvalues[i].contains(")"))
							{
								String qvalues_t = qvalues[i].replace(")", "");
								sql += qoper+"'"+qvalues_t+"')";
							}
							else
							{
								sql += qoper+"'"+qvalues[i]+"'";
							}
						}
						else
						{
							sql += qoper+qvalues[i];
						}
					}
					else
					{
						if(qvalues[i].contains(")"))
						{
							String qvalues_t = qvalues[i].replace(")", "");
							sql += qoper+" '%"+qvalues_t+"%')";
						}
						else
						{
							sql += qoper+" '%"+qvalues[i]+"%'";
						}
						
					}
				}
			}
			
			if(param.getBbox() != null && !"".equalsIgnoreCase(param.getBbox()))
			{
				if(!sql.contains(" where "))
				{
					sql += " where 1=1";
				}
				//空间数据查询
				if(DBHandle.getConnectionString().contains("oracle"))
				{
					String geoAction = param.getGeoaction();
					if("Intersects".equalsIgnoreCase(geoAction))
					{
						geoAction = "SDE.ST_INTERSECTS";
					}
					else if("Contains".equalsIgnoreCase(geoAction))
					{
						geoAction = "SDE.ST_CONTAINS";
					}
					else if("Within".equalsIgnoreCase(geoAction))
					{
						geoAction = "SDE.ST_WITHIN";
					}
					sql += " and "+geoAction+"(SDE.ST_GEOMETRY('"+param.getBbox()+"',4326),"+param.getGeofield()+")";
				}
				else if(DBHandle.getConnectionString().contains("postgresql"))
				{
					String geoAction = param.getGeoaction();
					if("Intersects".equalsIgnoreCase(geoAction))
					{
						geoAction = "ST_Intersects";
					}
					else if("Contains".equalsIgnoreCase(geoAction))
					{
						geoAction = "ST_Contains";
					}
					else if("Within".equalsIgnoreCase(geoAction))
					{
						geoAction = "ST_Within";
					}
					sql += " and "+geoAction+"('"+param.getBbox()+"'::geometry,"+param.getGeofield()+")";
				}
			}
			
			if(param.getUsersql()!= null && !"".equalsIgnoreCase(param.getUsersql()))
			{
				if(param.getUsersql().startsWith("*"))
				{
					sql = param.getUsersql().replaceFirst("\\*", "");
				}
				else
				{
					if(!sql.contains(" where "))
					{
						sql += " where 1=1";
					}
					sql += " and "+param.getUsersql();
					//自定义sql传值参数
					if(sql.contains("{") && sql.contains("}"))
					{
						
					}
				}
			}
			
			if(param.getGroupfield()!= null && !"".equalsIgnoreCase(param.getGroupfield()))
			{
				sql += " group by "+param.getGroupfield();
			}
			
			if(param.getOrderfields()!= null && !"".equalsIgnoreCase(param.getOrderfields()))
			{
				sql += " order by "+param.getOrderfields();
			}
			
			//执行SQL
			System.out.println("exeSelectSQL:"+sql);
			List<SuperObject> rs = DBHandle.exeSQLSelect2(sql, param.getStart(), param.getCount(), param.getIdfield(), param.getGeofield());
			//保存所有记录数
			if(rs != null && !resultSize.containsKey(param.toString()))
			{
				long rsize = DBHandle.getCount(sql);
				resultSize.add(param.toString(), rsize);
			}
			
			return rs;
		}
		catch(Exception e)
		{
			LogUtil.error(e);
			VisitControl.log(param, e, LogUtil.Level.WARN, "Data");
			return null;
		}
		
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#group(com.idata.core.DataParam)
	 */
	@Override
	public List<SuperObject> group(DataParam param) {
		// TODO Auto-generated method stub
		try
		{
			//数据库连接处理
			DBHandle = getDBHandle(param);
			
			String sql = null;
			//分组参数的处理
			if(param.getGroupfield() != null && !"".equalsIgnoreCase(param.getGroupfield()))
			{
				sql = "select ";
				String[] groupfields = param.getGroupfield().split(",");
				for(int i=0;i<groupfields.length;i++)
				{
					sql += groupfields[i]+",count("+groupfields[i]+") as count_"+groupfields[i]+",";
				}
				if(sql.endsWith(","))
				{
					sql = sql.substring(0, sql.length() - 1); 
				}
			}
			else
			{
				sql = "select";
			}
			if(param.getSumfield() != null && !"".equalsIgnoreCase(param.getSumfield()))
			{
				//求和字段
				String[] sumfields = param.getSumfield().split(",");
				for(int i=0;i<sumfields.length;i++)
				{
					if("select".equalsIgnoreCase(sql))
					{
						sql += " sum("+sumfields[i]+") as sum_"+sumfields[i];
					}
					else
					{
						sql += ",sum("+sumfields[i]+") as sum_"+sumfields[i];
					}
				}
			}
			
			sql += " from "+param.getLayer();
			
			if(param.getQueryfields() != null && !"".equalsIgnoreCase(param.getQueryfields()))
			{
				if(!sql.contains(" where "))
				{
					sql += " where 1=1";
				}
				
				String[] qfields = param.getQueryfields().split(",");
				String[] qopers = param.getQueryoperates().split(",");
				String[] keywords = param.getQueryvalues().split(",");
				String[] qrelas = param.getQueryrelations().split(",");
				for(int i=0;i<qfields.length;i++)
				{
					String qoper = null;
					if((i>qopers.length-1) || "".equalsIgnoreCase(qopers[i]))
					{
						qoper = "=";
					}
					else
					{
						qoper = qopers[i];
					}
					String qrela = null;
					if((i>qrelas.length-1) || "".equalsIgnoreCase(qrelas[i]))
					{
						qrela = "and";
					}
					else
					{
						qrela = qrelas[i];
					}
					
					sql += " "+qrela+" "+qfields[i];
					if(!"like".equalsIgnoreCase(qoper))
					{
						if("=".equalsIgnoreCase(qoper) || "!=".equalsIgnoreCase(qoper))
						{
							sql += qoper+"'"+keywords[i]+"'";
						}
						else
						{
							sql += qoper+keywords[i];
						}
					}
					else
					{
						sql += qoper+"'%"+keywords[i]+"%'";
					}
				}
			}
			
			if(param.getBbox() != null && !"".equalsIgnoreCase(param.getBbox()))
			{
				if(!sql.contains(" where "))
				{
					sql += " where 1=1";
				}
				//空间数据查询
				sql += " and "+param.getGeoaction()+"('"+param.getBbox()+"'::geometry,"+param.getGeofield()+")";
			}
			
			if(param.getUsersql()!= null && !"".equalsIgnoreCase(param.getUsersql()))
			{
				if(!sql.contains(" where "))
				{
					sql += " where 1=1";
				}
				sql += " and "+param.getUsersql();
			}
			
			if(param.getGroupfield() != null && !"".equalsIgnoreCase(param.getGroupfield()))
			{
				sql += " group by "+param.getGroupfield();
			}
			
			if(param.getOrderfields()!= null && !"".equalsIgnoreCase(param.getOrderfields()))
			{
				sql += " order by "+param.getOrderfields();
			}
			
			//执行SQL
			System.out.println("exeGroupSQL:"+sql);
			List<SuperObject> rs = DBHandle.exeSQLSelect2(sql, param.getStart(), param.getCount(), param.getIdfield(), param.getGeofield());
			return rs;		
		}
		catch(Exception e)
		{
			LogUtil.error(e);
			VisitControl.log(param, e, LogUtil.Level.WARN, "Data");
			return null;
		}
		
	}

	@Override
	public List<SuperObject> getMeta(DataParam param) {
		// TODO Auto-generated method stub
		try
		{
			List<SuperObject> rsd = metaInfos.getObject(param.getkey());
			if(rsd == null)
			{
				//数据库连接处理
				DBHandle = getDBHandle(param);
				if(param.getLayer() != null && !"".equalsIgnoreCase(param.getLayer()))
				{
					String sql = "select * from "+param.getLayer();
					System.out.println("exeMetaSQL:"+sql);
					rsd = DBHandle.getMetaData2(sql);
				}
				metaInfos.add(param.getkey(), rsd);
			}
			return rsd;
		}
		catch(Exception e)
		{
			LogUtil.error(e);
			VisitControl.log(param, e, LogUtil.Level.WARN, "Data");
			return null;
		}
		
	}

	@Override
	public boolean isSupport() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public int getClassOrder() {
		// TODO Auto-generated method stub
		return 2;
	}

	@Override
	public boolean isSupport(String fun) {
		// TODO Auto-generated method stub
		switch(fun){
		  case "add" :
			  return true;
		  case "adds" :
			  return true;
	      case "edit" :
	    	  return true; 
	      case "delete" :
	    	  return true; 
	      case "query" :
	    	  return true; 
	      case "group" :
	    	  return true; 
	      case "getmeta" :
	    	  return true; 
	      default : 
	          return false;
	    }
	}


}
