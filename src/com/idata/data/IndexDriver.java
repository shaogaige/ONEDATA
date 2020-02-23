/**
 * ClassName:IndexDriver.java 
 * Date:2020年1月19日
 */
package com.idata.data;

import java.util.List;

import com.idata.core.DataParam;
import com.idata.core.SuperObject;
import com.idata.core.TableIndexOperator;
import com.idata.tool.PropertiesUtil;

/**
 * Creater:SHAO Gaige
 * Description:
 * Log:
 */
public class IndexDriver implements IDataDriver {
	
	TableIndexOperator indexOperator = new TableIndexOperator();

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#add(com.idata.core.DataParam)
	 */
	@Override
	public boolean add(DataParam param) {
		// TODO Auto-generated method stub
		return indexOperator.add(param);
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#edit(com.idata.core.DataParam)
	 */
	@Override
	public boolean edit(DataParam param) {
		// TODO Auto-generated method stub
		return indexOperator.edit(param);
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#delete(com.idata.core.DataParam)
	 */
	@Override
	public boolean delete(DataParam param) {
		// TODO Auto-generated method stub
		return indexOperator.delete(param);
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#query(com.idata.core.DataParam)
	 */
	@Override
	public List<SuperObject> query(DataParam param) {
		// TODO Auto-generated method stub
		return indexOperator.query(param);
	}

	/* (non-Javadoc)
	 * @see com.idata.data.IDataDriver#group(com.idata.core.DataParam)
	 */
	@Override
	public List<SuperObject> group(DataParam param) {
		// TODO Auto-generated method stub
		return indexOperator.group(param);
	}

	@Override
	public List<SuperObject> getMeta(DataParam param) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isSupport() {
		// TODO Auto-generated method stub
		return Boolean.getBoolean(PropertiesUtil.getValue("INDEXSUPPORT"));
	}

}
