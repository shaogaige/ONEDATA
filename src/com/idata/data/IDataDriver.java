/**
 * ClassName:IDataDriver.java 
 * Date:2020年1月19日
 */
package com.idata.data;

import java.util.List;

import com.idata.core.DataParam;
import com.idata.core.SuperObject;
import com.idata.tool.TWBCacheManager;

/**
 * Creater:SHAO Gaige
 * Description:数据源驱动接口
 * Log:
 */
public interface IDataDriver {
	
	//缓存表记录数
	public static TWBCacheManager<Long> resultSize = new TWBCacheManager<Long>();
	//缓存表元数据
	public static TWBCacheManager<List<SuperObject>> metaInfos = new TWBCacheManager<List<SuperObject>>();
	
	/**
	 * 新增记录
	 * @param DataParam
	 * @return boolean
	 */
	public boolean add(DataParam param);
	
	/**
	 * 批量新增
	 * @param param
	 * @return boolean
	 */
	public boolean adds(DataParam param);
	
	/**
	 * 编辑记录
	 * @param DataParam
	 * @return boolean
	 */
	public boolean edit(DataParam param);
	
	/**
	 * 删除记录
	 * @param param
	 * @return boolean
	 */
	public boolean delete(DataParam param);
	
	/**
	 * 查询过滤
	 * @param DataParam
	 * @return String
	 */
	public List<SuperObject> query(DataParam param);
	
	/**
	 * 统计分析
	 * @param DataParam
	 * @return String
	 */
	public List<SuperObject> group(DataParam param);
	
	/**
	 * 获取元数据
	 * @param DataParam
	 * @return String
	 */
	public List<SuperObject> getMeta(DataParam param);
	
	/**
	 * 是否支持此类型
	 * @return boolean
	 */
	public boolean isSupport();
	/**
	 * 是否此函数
	 * @return boolean
	 */
	public boolean isSupport(String fun);
	
	/**
	 * 类排序
	 * @return int
	 */
	public int getClassOrder();
}
