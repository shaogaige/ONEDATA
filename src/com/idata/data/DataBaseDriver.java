/**
 * ClassName:DataBaseDriver.java 
 * Date:2020年1月19日
 */
package com.idata.data;

import java.sql.Types;
import java.util.List;

import com.idata.core.DataBaseHandle;
import com.idata.core.DataParam;
import com.idata.core.SuperObject;
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
		if(param.getCon() != null && !"".equalsIgnoreCase(param.getCon()))
		{
			try 
			{
				DBHandle = new DataBaseHandle(param.getCon());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		else if(param.getUid() != null && !"".equalsIgnoreCase(param.getUid()))
		{
			//使用ID获取数据库连接
			
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
			List<SuperObject> rsd = this.getMeta(param);
			int size = rsd.size();
			String sql = "insert into "+param.getLayer()+ "(";
			String values = "(";
			PreparedParam preparedParam = new PreparedParam();
			int index = 1;
			//获取所有列名称
			for(int i=0;i<size;i++)
			{
				String columnName = rsd.get(i).getProperty("name").getString_value();
				if(columnName.equalsIgnoreCase(param.getIdfield()))
				{
					//id
					int type = rsd.get(i).getProperty("type").getInt_value();
					if(type==Types.VARCHAR||type==Types.LONGVARCHAR||type==Types.NCHAR||
							type==Types.NVARCHAR||type==Types.LONGNVARCHAR)
					{
						int p = rsd.get(i).getProperty("precision").getInt_value();
						String id = new StringBuffer(RandomIDUtil.getUUID("")).reverse().toString();
						int max = p>id.length()?id.length():p;
						id = id.substring(0, max);
						id = new StringBuffer(id).reverse().toString();
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
					//？空间字段的处理?
					sql += columnName+",";
					values += "?,";
					
					preparedParam.addParam(index, v);
				}
				index++;
			}
			sql = sql.substring(0, sql.length()-1)+") values";
			values = values.substring(0, values.length()-1)+")";
			sql = sql+values;
			System.out.println("exeAddSQL:"+sql);
			DBHandle.exePreparedSQLInsert(sql, preparedParam);
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			//获取所有列名称??是否应该按上传的字段？
			for(int i=0;i<size;i++)
			{
				String columnName = rsd.get(i).getProperty("name").getString_value();
				Value v = data.getProperty(columnName);
				if(v == null)
				{
					continue;
				}
				//?空间字段的处理？
				sql += columnName+"=?,";
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
				String[] keywords = param.getKeywords().split(",");
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
			
			if(param.getUsersql()!= null && !"".equalsIgnoreCase(param.getUsersql()))
			{
				if(!sql.contains(" where "))
				{
					sql += " where 1=1";
				}
				sql += " and "+param.getUsersql();
			}
			System.out.println("exeEditSQL:"+sql);
			DBHandle.exePreparedSQLUpdate(sql, preparedParam);
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				String[] keywords = param.getKeywords().split(",");
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
				System.out.println("exeDeleteSQL:"+sql);
				return DBHandle.exeSQLDelete(sql);
			}
			else
			{
				return false;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			DBHandle = getDBHandle(param);
			//使用id查询的如何处理？
			String sql = null;
			//查询参数的处理
			if(param.getOutFields() == null || "".equalsIgnoreCase(param.getOutFields()))
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
								field = "sde.st_astext("+param.getGeofield()+") as GEOMETRY";
							}
							else if(DBHandle.getConnectionString().contains("postgresql"))
							{
								field = "ST_AsText("+param.getGeofield()+") as GEOMETRY";
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
				sql = "select "+param.getOutFields()+" from "+param.getLayer();
				if(param.getGeofield() != null && !"".equalsIgnoreCase(param.getGeofield()))
				{
					if(param.getOutFields().contains(param.getGeofield()))
					{
						String str = "";
						if(DBHandle.getConnectionString().contains("oracle"))
						{
							str = "sde.st_astext("+param.getGeofield()+") as GEOMETRY";
						}
						else if(DBHandle.getConnectionString().contains("postgresql"))
						{
							str = "ST_AsText("+param.getGeofield()+") as GEOMETRY";
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
				String[] keywords = param.getKeywords().split(",");
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
			
			//执行SQL
			System.out.println("exeSelectSQL:"+sql);
			List<SuperObject> rs = DBHandle.exeSQLSelect2(sql, param.getStart(), param.getCount(), param.getIdfield(), param.getGeofield());
			//保存所有记录数
			if(rs != null && !resultSize.containsKey(param.toString()))
			{
				long rsize = DBHandle.getCount(sql);
				resultSize.put(param.toString(), rsize);
			}
			
			return rs;
		}catch(Exception e)
		{
			e.printStackTrace();
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
				
				sql = "select "+param.getGroupfield()+",count("+param.getGroupfield()+") as count_"+param.getGroupfield();
			}
			else
			{
				return null;
			}
			if(param.getSumfield() != null && !"".equalsIgnoreCase(param.getSumfield()))
			{
				//求和字段
				String[] sumfields = param.getSumfield().split(",");
				for(int i=0;i<sumfields.length;i++)
				{
					sql += ",sum("+sumfields[i]+") as sum_"+sumfields[i];
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
				String[] keywords = param.getKeywords().split(",");
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
			sql += " group by "+param.getGroupfield();
			//执行SQL
			System.out.println("exeGroupSQL:"+sql);
			List<SuperObject> rs = DBHandle.exeSQLSelect2(sql, param.getStart(), param.getCount(), param.getIdfield(), param.getGeofield());
			return rs;		
		}catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
		
	}

	@Override
	public List<SuperObject> getMeta(DataParam param) {
		// TODO Auto-generated method stub
		try
		{
			//数据库连接处理
			DBHandle = getDBHandle(param);
			if(param.getLayer() != null && !"".equalsIgnoreCase(param.getLayer()))
			{
				String sql = "select * from "+param.getLayer();
				System.out.println("exeMetaSQL:"+sql);
				List<SuperObject> rsd = DBHandle.getMetaData2(sql);
				return rsd;
			}
			else
			{
				//其他自定义SQL
				return null;
			}
			
		}catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
		
	}

	@Override
	public boolean isSupport() {
		// TODO Auto-generated method stub
		return true;
	}

}
