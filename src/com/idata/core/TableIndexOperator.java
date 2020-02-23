/**
 * ClassName:TableIndexOperator.java
 * Date:2019年4月2日
 */
package com.idata.core;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.geotools.geojson.GeoJSON;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;

import com.idata.data.IDataDriver;
import com.ojdbc.sql.Value;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Creater:SHAO Gaige
 * Description:表格索引操作类
 * Log:
 */
public class TableIndexOperator {
	
	private WKTReader OGCWKTReader = new WKTReader();
	
	/**
	 * 构造函数
	 */
	public TableIndexOperator()
	{
		
	}
	
	/**
	 * 新增索引
	 * @param filter
	 * @param path
	 * @return
	 */
	public boolean add(DataParam filter)
	{
		IndexWriter writer = null;
		try 
		{
			Document doc = buildDoc(filter);
			if(doc != null) 
			{
				// 定义索引输出目录
				Directory dir = FSDirectory.open(Paths.get(filter.getPath()));
				IndexWriterConfig iwc = new IndexWriterConfig(TableIndexWriter.analyzer);
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				// 创建write对象
				writer = new IndexWriter(dir, iwc);
				writer.addDocument(doc);
				writer.commit();
				return true;
			}
			else
			{
				return false;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			writer = null;
		}
		
	}
	/**
	 * 编辑索引
	 * @param filter
	 * @param path
	 * @return
	 */
	public boolean edit(DataParam filter)
	{
		IndexWriter writer = null;
		try 
		{
			Document doc = buildDoc(filter);
			if(doc != null) 
			{
				// 定义索引输出目录
				Directory dir = FSDirectory.open(Paths.get(filter.getPath()));
				IndexWriterConfig iwc = new IndexWriterConfig(TableIndexWriter.analyzer);
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				// 创建write对象
				writer = new IndexWriter(dir, iwc);
				
				IndexableField ifield = doc.getField(filter.getIdfield());
				Term term = null;
				if(ifield.fieldType().docValuesType()==DocValuesType.NUMERIC)
				{
					term = new Term(filter.getIdfield(),String.valueOf(ifield.numericValue()));
				}
				else
				{
					term = new Term(filter.getIdfield(),ifield.stringValue());
				}
				writer.updateDocument(term, doc);
				writer.commit();
				return true;
			}
			else
			{
				return false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			writer = null;
		}
		
	}
	
	/**
	 * 删除索引
	 * @param filter
	 * @param path
	 * @return
	 */
	public boolean delete(DataParam filter)
	{
		IndexWriter writer = null;
		try 
		{
			Document doc = buildDoc(filter);
			if(doc != null) 
			{
				// 定义索引输出目录
				Directory dir = FSDirectory.open(Paths.get(filter.getPath()));
				IndexWriterConfig iwc = new IndexWriterConfig(TableIndexWriter.analyzer);
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				// 创建write对象
				writer = new IndexWriter(dir, iwc);
				
				IndexableField ifield = doc.getField(filter.getIdfield());
				Term term = null;
				if(ifield.fieldType().docValuesType()==DocValuesType.NUMERIC)
				{
					term = new Term(filter.getIdfield(),String.valueOf(ifield.numericValue()));
				}
				else
				{
					term = new Term(filter.getIdfield(),ifield.stringValue());
				}
				writer.deleteDocuments(term);
				writer.commit();
				return true;
			}
			else
			{
				return false;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			writer = null;
		}
	}
	/**
	 * 查询索引
	 * @param filter
	 * @return
	 */
	public List<SuperObject> query(DataParam filter)
	{
		try
		{
			BooleanQuery query_all = buildQuery(filter);
	        System.out.println("Query:"+query_all.toString());
	        IndexSearcher searcher = OneDataServer.getTableSeracher(filter.getLayer());
			if(searcher != null)
			{
				TopDocs results = searcher.search(query_all, searcher.getIndexReader().maxDoc());
				
				long hit = results.totalHits;
				System.out.println("result size:"+hit);
				if(hit < 1)
				{
					System.out.println("标准查询无结果,启动系统模糊查询...");
					query_all = buildWildFilter(filter);
					results = searcher.search(query_all, 10);
				}
				return resultBuildQuery(results,filter,searcher);
			}
			return null;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
		
	}
	
	/**
	 * 查询索引
	 * @param filter
	 * @return
	 * @throws Exception
	 */
	public String search(DataParam filter) throws Exception
	{
		BooleanQuery query_all = buildQuery(filter);
        System.out.println("Query:"+query_all.toString());
        IndexSearcher searcher = OneDataServer.getTableSeracher(filter.getLayer());
		if(searcher != null)
		{
			TopDocs results = searcher.search(query_all, searcher.getIndexReader().maxDoc());
			
			long hit = results.totalHits;
			System.out.println("result size:"+hit);
			if(hit < 1)
			{
				System.out.println("标准查询无结果,启动系统模糊查询...");
				query_all = buildWildFilter(filter);
				results = searcher.search(query_all, 10);
			}
			return resultBuild(results,filter,searcher);
		}
		return null;
	}
	/**
	 * 统计分组
	 * @param filter
	 * @return
	 */
	public List<SuperObject> group(DataParam filter)
	{
		BooleanQuery query_all = buildQuery(filter);
		
		IndexSearcher searcher = OneDataServer.getTableSeracher(filter.getLayer());
		
		GroupQuery groupQuery = new GroupQuery(query_all,filter,searcher);
		
		return resultBuildGroup(searcher,groupQuery,filter);
	}
	
	/**
	 * 分组统计计算
	 * @param filter
	 * @return GroupQuery
	 */
	public String group2(DataParam filter)
	{
		BooleanQuery query_all = buildQuery(filter);
		
		IndexSearcher searcher = OneDataServer.getTableSeracher(filter.getLayer());
		
		GroupQuery groupQuery = new GroupQuery(query_all,filter,searcher);
		
		return resultBuild(searcher,groupQuery,filter);
	}
	//创建document
	private Document buildDoc(DataParam param)
	{
		try
		{
			// 创建 Document
			Document doc = new Document();
			SuperObject data = new SuperObject();
			data.setJSONString(param.getJsondata(), param.getIdfield(), param.getGeofield(),true);
			//输出字段
			if("normal".equalsIgnoreCase(data.getType()))
			{
				Field JSONDATA = new TextField("ALL_JSON_DATA", data.getJSONString("json", null), Field.Store.YES);
				doc.add(JSONDATA);
			}
			else
			{
				Field JSONDATA = new TextField("ALL_JSON_DATA", data.getJSONString("geojson", null), Field.Store.YES);
				doc.add(JSONDATA);
				//空间字段
				org.locationtech.spatial4j.io.WKTReader wktReader = new org.locationtech.spatial4j.io.WKTReader(
						TableIndexWriter.ctx, TableIndexWriter.fac);
				Geometry geometry = data.getGeo();
				if (geometry != null)
				{
					StringWriter output = new StringWriter();
					GeoJSON.write(geometry, output);

					// Shape point = wktReader.parse(wkt);
					Shape shape = null;
					String geotype = geometry.getGeometryType();
					//System.out.println("geotype:"+geotype);
					if ("point".equalsIgnoreCase(geotype))
					{
						shape = wktReader.parse(data.getGeo_wkt());
					}
					else
					{
						Envelope env = geometry.getEnvelopeInternal();
						shape = new RectangleImpl(env.getMinX(), env.getMaxX(), env.getMinY(),
								env.getMaxY(), TableIndexWriter.ctx);
					}
					for (Field f :TableIndexWriter.strategy.createIndexableFields(shape))
					{
						doc.add(f);
					}
					// 存储信息,用于最后过滤
					Field GEOMETRY_FIELD = new TextField("GEOMETRY_FIELD", geometry.toText(),
							Field.Store.YES);
					doc.add(GEOMETRY_FIELD);
				}
			}
			//属性字段
			Set<String> keys = data.getKeys();
			for(String key:keys)
			{
				Value v = data.getProperty(key);
				if(v.isStringValue())
				{
					Field filed = new TextField(key, v.getString_value(), Field.Store.YES);
					doc.add(filed);
					
					Field sortfield = new SortedDocValuesField(key, new BytesRef(v.getString_value()));
					doc.add(sortfield);
				}
				else if(v.isDoubleValue() || v.isFloatValue() || v.isIntValue()
						|| v.isLongValue())
				{
					Field field = new DoublePoint(key,v.getDouble_value());
					doc.add(field);
					doc.add(new StoredField(key,v.getDouble_value()));//存储
					doc.add(new DoubleDocValuesField(key,v.getDouble_value()));//排序
				}
				else if(v.isBooleanValue())
				{
					Field filed = new TextField(key, String.valueOf(v.getBoolean_value()), Field.Store.YES);
					doc.add(filed);
					
					Field sortfield = new SortedDocValuesField(key, new BytesRef(String.valueOf(v.getBoolean_value())));
					doc.add(sortfield);
				}
				else
				{
					Field filed = new TextField(key, v.getString_value(), Field.Store.YES);
					doc.add(filed);
					
					Field sortfield = new SortedDocValuesField(key, new BytesRef(v.getString_value()));
					doc.add(sortfield);
				}
			}
			return doc;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}
	
	private BooleanQuery buildQuery(DataParam filter)
	{
		BooleanQuery.Builder builder_all = new BooleanQuery.Builder();
		
		//一般查询
		buildFilter(filter,builder_all);
		
		//空间查询
		buildGeometryFilter(filter,builder_all);
		
		//其他过滤条件,自定义查询
		buildCustomQueryFilter(filter.getUsersql(),builder_all);
		
		return builder_all.build();
		
	}
	
	private void buildFilter(DataParam filter,BooleanQuery.Builder builder_all)
	{
		String keyWord = filter.getKeywords();
		if(keyWord != null && !"".equalsIgnoreCase(keyWord))
		{
			
			String field = filter.getOutFields();
			if(field == null || "".equalsIgnoreCase(field))
			{
				field = "ALL_JSON_DATA";
			}
			
			Term term1 = new Term(field,keyWord);
			Query query1 = new TermQuery(term1);
			//builder_keyword.add(query1, Occur.MUST);
			
			builder_all.add(query1,Occur.MUST);
		}
	}
	
	private BooleanQuery buildWildFilter(DataParam filter)
	{
		BooleanQuery.Builder builder_all = new BooleanQuery.Builder();
		String keyWord = filter.getKeywords();
		if(keyWord != null && !"".equalsIgnoreCase(keyWord))
		{
			BooleanQuery.Builder builder_keyword = new BooleanQuery.Builder();
			
			String field = filter.getOutFields();
			if(field == null || "".equalsIgnoreCase(field))
			{
				field = "ALL_JSON_DATA";
			}
			
			Term term1 = new Term(field,"*"+keyWord+"*");
			Query query1 = new WildcardQuery(term1);
			builder_keyword.add(query1, Occur.MUST);
		}
		//空间查询
		buildGeometryFilter(filter,builder_all);
				
		//其他过滤条件,自定义查询
		buildCustomQueryFilter(filter.getUsersql(),builder_all);
		
		return builder_all.build();
	}
	
	private void buildGeometryFilter(DataParam filter,BooleanQuery.Builder builder_all)
	{
		String geowkt = filter.getBbox();
		if(geowkt != null && !"".equalsIgnoreCase(geowkt))
		{
			try 
			{
				Geometry geo = OGCWKTReader.read(geowkt);
				if(geo == null) {
					return;
				}
				//坐标转换
				//geo = JTS.transform(geo, transform);
			    filter.setFilterGeometry(geo);
				Envelope env = geo.getEnvelopeInternal();
				Shape shape = TableIndexWriter.ctx.getShapeFactory().rect(env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY());
				
				SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, shape);
		        Query query = TableIndexWriter.strategy.makeQuery(args);
		        
		        builder_all.add(query, Occur.MUST);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	//自定义查询条件
	private void buildCustomQueryFilter(String customQuery,BooleanQuery.Builder builder_all)
	{
		if(customQuery != null && !"".equalsIgnoreCase(customQuery) && customQuery.contains(":"))
		{
			String[] customQuerys = customQuery.trim().split(",");
			for(int i=0;i<customQuerys.length;i++)
			{
				if(!customQuerys[i].contains(":")) {
					continue;
				}
				String queryField = customQuerys[i].substring(0, customQuerys[i].indexOf(":"));
				String value = customQuerys[i].substring(customQuerys[i].indexOf(":")+1);
				boolean unoperation = false;
				if(customQuerys[i].contains("!"))
				{
					unoperation = true;
					value = value.substring(1);
				}
				
				if(value.contains("*"))
				{
					Term term1 = new Term(queryField,value);
					Query query1 = new WildcardQuery(term1);
					if(!unoperation)
					{
						builder_all.add(query1, Occur.MUST);
					}
					else
					{
						builder_all.add(query1, Occur.MUST_NOT);
					}
				}
				else
				{
					Term term2 = new Term(queryField,value);
					Query query2 = new TermQuery(term2);
					if(!unoperation)
					{
						builder_all.add(query2, Occur.MUST);
					}
					else
					{
						builder_all.add(query2, Occur.MUST_NOT);
					}
				}
			}
		}
	}
	//group结果集创建
	private List<SuperObject> resultBuildGroup(IndexSearcher searcher,GroupQuery groupQuery,DataParam filter)
	{
		//分组
		try
		{
			TopGroups<BytesRef> groups = groupQuery.group();
			int length = groups.groups.length;
			System.out.println("group size:"+length);
			boolean f_sum = false;
			if(filter.getSumfield() != null && !"".equalsIgnoreCase(filter.getSumfield()))
			{
				f_sum = true;
			}
			List<SuperObject> rs = new ArrayList<SuperObject>();
			for(GroupDocs<BytesRef> groupDocs:groups.groups)
			{
				BytesRef br = groupDocs.groupValue;
				if(br == null || "".equalsIgnoreCase(br.utf8ToString())) {
					continue;
				}
				String key = br.utf8ToString();
				//System.out.println(key);
				key = key.trim();
				//BytesRef gsv = (BytesRef) groupDocs.groupSortValues[0];
				//System.out.println(gsv.utf8ToString());
				long value = groupDocs.totalHits;
				
				double sum = 0.0;
				if(f_sum)
				{
					//System.out.println("length:"+groupDocs.scoreDocs.length);
					for(ScoreDoc scoreDoc : groupDocs.scoreDocs)
					{
						Document doc = searcher.doc(scoreDoc.doc);
						String v = doc.get(filter.getSumfield());
						//System.out.println(v);
						if(isDouble(v))
						{
							sum += Double.valueOf(v);
						}
					}
				}
				SuperObject o = new SuperObject();
				o.setJSONString("[\""+key+"\","+value+","+sum+"]", null, null, false);
				rs.add(o);
			}
			
			//System.out.println(sb.toString());
			return rs;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	private String resultBuild(IndexSearcher searcher,GroupQuery groupQuery,DataParam filter)
	{
		//分组
		try
		{
			TopGroups<BytesRef> groups = groupQuery.group();
			int length = groups.groups.length;
			System.out.println("group size:"+length);
			boolean f_sum = false;
			if(filter.getSumfield() != null && !"".equalsIgnoreCase(filter.getSumfield()))
			{
				f_sum = true;
			}
			StringBuffer sb = new StringBuffer("{\"state\":true,\"data\":[");
			for(GroupDocs<BytesRef> groupDocs:groups.groups)
			{
				BytesRef br = groupDocs.groupValue;
				if(br == null || "".equalsIgnoreCase(br.utf8ToString())) {
					continue;
				}
				String key = br.utf8ToString();
				//System.out.println(key);
				key = key.trim();
				//BytesRef gsv = (BytesRef) groupDocs.groupSortValues[0];
				//System.out.println(gsv.utf8ToString());
				long value = groupDocs.totalHits;
				
				double sum = 0.0;
				if(f_sum)
				{
					//System.out.println("length:"+groupDocs.scoreDocs.length);
					for(ScoreDoc scoreDoc : groupDocs.scoreDocs)
					{
						Document doc = searcher.doc(scoreDoc.doc);
						String v = doc.get(filter.getSumfield());
						//System.out.println(v);
						if(isDouble(v))
						{
							sum += Double.valueOf(v);
						}
					}
				}
				sb.append("[\""+key+"\","+value+","+sum+"],");
			}
			if(',' == sb.charAt(sb.length()-1))
			{
				sb = sb.deleteCharAt(sb.length()-1);
			}
			sb.append("]}");
			//System.out.println(sb.toString());
			return sb.toString();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "{\"state\":true,\"data\":[]}";
		}
	}
	
	private List<SuperObject> resultBuildQuery(TopDocs results,DataParam filter,IndexSearcher searcher) throws Exception
	{
		if(results == null)
		{
			return null;
		}
		
		ScoreDoc[] hits = results.scoreDocs;
		int size = hits.length;
		System.out.println("size:"+size);
		
		//返回详细信息
		int start = 0;
		if(filter.getStart()>0 && filter.getStart()<=size) 
		{
			start = filter.getStart();
		}
		
		int count = size;
		if(filter.getCount()>0 && ((filter.getCount()+start)<size))
		{
			count = filter.getCount()+start;
		}
		PreparedGeometry mainGeo = filter.getFilterPreparedGeometry();
		String spatialRelation = filter.getGeoaction();
		//耗时
		int k = 0;
		int resultsize = size;
		List<SuperObject> rs = new ArrayList<SuperObject>();
		for(int i=0;i<size;i++)
		{
			Document doc = searcher.doc(hits[i].doc);
			if(mainGeo != null)
			{
				Geometry geo = OGCWKTReader.read(doc.get("GEOMETRY_FIELD"));
				if("Intersects".equalsIgnoreCase(spatialRelation))
				{
					if(!mainGeo.intersects(geo))
					{
						resultsize--;
						continue;
					}
				}
				else if("Contains".equalsIgnoreCase(spatialRelation))
				{
					if(!mainGeo.contains(geo))
					{
						resultsize--;
						continue;
					}
				}
				else if("Within".equalsIgnoreCase(spatialRelation))
				{
					if(!mainGeo.within(geo))
					{
						resultsize--;
						continue;
					}
				}
			}
			k++;
			if(k >= start)
			{
				if(k<count)
				{
					String data = doc.get("ALL_JSON_DATA");
					SuperObject o = new SuperObject();
					o.setJSONString(data, null, null, false);
					rs.add(o);
				}
				else
				{
					break;
				}
			}
		}
		IDataDriver.resultSize.put(filter.toString(), (long) resultsize);
		return rs;
	}
	
	private String resultBuild(TopDocs results,DataParam filter,IndexSearcher searcher) throws Exception
	{
		if(results == null)
		{
			return "{\"state\":true,\"resultsize\":0,\"data\":[]}";
		}
		
		ScoreDoc[] hits = results.scoreDocs;
		int size = hits.length;
		System.out.println("size:"+size);
		StringBuffer sb = new StringBuffer("");
		if("geojson".equalsIgnoreCase(filter.getOut()))
		{
			sb.append("{\"state\":true,\"data\":[");
			//返回详细信息
			int start = 0;
			if(filter.getStart()>0 && filter.getStart()<=size) 
			{
				start = filter.getStart();
			}
			
			int count = size;
			if(filter.getCount()>0 && ((filter.getCount()+start)<size))
			{
				count = filter.getCount()+start;
			}
			PreparedGeometry mainGeo = filter.getFilterPreparedGeometry();
			String spatialRelation = filter.getGeoaction();
			//耗时
			int k = 0;
			int resultsize = size;
			for(int i=0;i<size;i++)
			{
				Document doc = searcher.doc(hits[i].doc);
				if(mainGeo != null)
				{
					Geometry geo = OGCWKTReader.read(doc.get("GEOMETRY_FIELD"));
					if("Intersects".equalsIgnoreCase(spatialRelation))
					{
						if(!mainGeo.intersects(geo))
						{
							resultsize--;
							continue;
						}
					}
					else if("Contains".equalsIgnoreCase(spatialRelation))
					{
						if(!mainGeo.contains(geo))
						{
							resultsize--;
							continue;
						}
					}
					else if("Within".equalsIgnoreCase(spatialRelation))
					{
						if(!mainGeo.within(geo))
						{
							resultsize--;
							continue;
						}
					}
				}
				k++;
				if(k >= start)
				{
					if(k<count)
					{
						String data = doc.get("ALL_JSON_DATA");
						sb.append(data);
						sb.append(",");
					}
					else
					{
						break;
					}
				}
			}
			if(',' == sb.charAt(sb.length()-1))
			{
				sb = sb.deleteCharAt(sb.length()-1);
			}
			sb.append("],\"resultsize\":\""+resultsize+"\"");
			sb.append("}");
		}
		return sb.toString();
	}
	
	//是否是数字
	private boolean isDouble(String str){
		try
		{
		    Double.parseDouble(str);
		    return true;
		}
		catch(Exception ex){
			return false;
		}
    }
	
}
