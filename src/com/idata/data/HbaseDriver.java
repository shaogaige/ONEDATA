/**
 * ClassName:HbaseDriver.java 
 * Date:2020年1月19日
 */
package com.idata.data;

import java.util.List;

import com.idata.core.DataParam;
import com.idata.core.HbaseOperator;
import com.idata.core.SuperObject;
import com.idata.tool.PropertiesUtil;

/**
 * Creater:SHAO Gaige
 * Description:
 * Log:
 */
public class HbaseDriver implements IDataDriver {
	
	private HbaseOperator hbaseOperator = new HbaseOperator();

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#add(com.idata.core.DataParam)
	 */
	@Override
	public boolean add(DataParam param) {
		// TODO Auto-generated method stub
		return hbaseOperator.add(param);
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#edit(com.idata.core.DataParam)
	 */
	@Override
	public boolean edit(DataParam param) {
		// TODO Auto-generated method stub
		return hbaseOperator.edit(param);
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#delete(com.idata.core.DataParam)
	 */
	@Override
	public boolean delete(DataParam param) {
		// TODO Auto-generated method stub
		return hbaseOperator.delete(param);
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#query(com.idata.core.DataParam)
	 */
	@Override
	public List<SuperObject> query(DataParam param) {
		// TODO Auto-generated method stub
		return hbaseOperator.query(param);
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#group(com.idata.core.DataParam)
	 */
	@Override
	public List<SuperObject> group(DataParam param) {
		// TODO Auto-generated method stub
		return hbaseOperator.group(param);
	}

	@Override
	public List<SuperObject> getMeta(DataParam param) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isSupport() {
		// TODO Auto-generated method stub
		return Boolean.getBoolean(PropertiesUtil.getValue("HBASESUPPORT"));
	}

	@Override
	public int getClassOrder() {
		// TODO Auto-generated method stub
		return 4;
	}

	@Override
	public boolean isSupport(String fun) {
		// TODO Auto-generated method stub
		switch(fun){
		  case "add" :
			  return true;
		  case "adds" :
			  return false;
	      case "edit" :
	    	  return true; 
	      case "delete" :
	    	  return true; 
	      case "query" :
	    	  return true; 
	      case "group" :
	    	  return true; 
	      case "getmeta" :
	    	  return false; 
	      default : 
	          return false;
	    }
	}

	@Override
	public boolean adds(DataParam param) {
		// TODO Auto-generated method stub
		return false;
	}

}
