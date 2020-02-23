/**
 * ClassName:GroupQuery.java
 * Date:2019年6月17日
 */
package com.idata.core;

import java.io.IOException;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;


/**
 * Creater:SHAO Gaige
 * Description:分组查询实现类
 * Log:
 */
public class GroupQuery {
	
    private BooleanQuery queryAll;
	
	private DataParam filter;
	
	private IndexSearcher searcher;
	
	public GroupQuery(BooleanQuery query,DataParam filter,IndexSearcher searcher)
	{
		this.queryAll = query;
		this.filter = filter;
		this.searcher = searcher;
	}
	
	public TopGroups<BytesRef> group() throws IOException
	{
		GroupingSearch groupingSearch = new GroupingSearch(this.filter.getGroupfield());
		//groupingSearch.setGroupSort(new Sort(new SortField("TYPECODE",SortField.Type.STRING_VAL)));
        //groupingSearch.setFillSortFields(true);
		//groupingSearch.setGroupSort(new Sort(SortField.FIELD_SCORE));
        //groupingSearch.setFillSortFields(true);
		int maxsize = this.searcher.getIndexReader().maxDoc();
		if(filter.getSumfield() != null && !"".equalsIgnoreCase(filter.getSumfield()))
		{
			groupingSearch.setGroupDocsLimit(maxsize);
		}
		groupingSearch.setCachingInMB(100.0, true);
		groupingSearch.setAllGroups(true);
		TopGroups<BytesRef> result;
		if(queryAll == null || "".equalsIgnoreCase(queryAll.toString()))
		{
			Query query = new MatchAllDocsQuery();
			result = groupingSearch.search(this.searcher,query, 0, maxsize);
		}
		else
		{
			result = groupingSearch.search(this.searcher,queryAll, 0, maxsize);
		}
		return result;
	}

}
