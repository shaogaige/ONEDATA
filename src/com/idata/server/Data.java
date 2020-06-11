package com.idata.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.idata.core.DataParam;
import com.idata.cotrol.DataControl;

/**
 * Servlet implementation class Data
 */
@WebServlet(description = "数据统一访问接口", urlPatterns = { "/Data" })
public class Data extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Data() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		DataParam param = new DataParam();
		//System.out.println(request.getQueryString());
		param.setParam_String(request.getQueryString());
		//唯一标识
		String uid = request.getParameter("uid");
		param.setUid(uid);
		//数据库连接字符串
		String con = request.getParameter("con");
		param.setCon(con);
		//图层名，表名
		String layer = request.getParameter("layer");
		if(layer != null) {
			layer = new String(layer.getBytes("ISO8859-1"),"UTF-8");
			layer = URLDecoder.decode(layer,"UTF-8");
			param.setLayer(layer);
		}
		//数据操作
		String operation = request.getParameter("operation");
		param.setOperation(operation);
		//分页
		String start = request.getParameter("start");
		param.setStart_s(start);
		
		String cout = request.getParameter("count");
		param.setCount_s(cout);
		//实体数据
		String jsondata = request.getParameter("jsondata");
		if(jsondata != null) {
			jsondata = new String(jsondata.getBytes("ISO8859-1"),"UTF-8");
			jsondata = URLDecoder.decode(jsondata,"UTF-8");
			param.setJsondata(jsondata);
		}
		//输出字段
		String outfields = request.getParameter("outfields");
		if(outfields != null) {
			outfields = new String(outfields.getBytes("ISO8859-1"),"UTF-8");
			outfields = URLDecoder.decode(outfields,"UTF-8");
			param.setOutFields(outfields);
		}
		//字段名
		String queryfields = request.getParameter("queryfields");
		if(queryfields != null) {
			queryfields = new String(queryfields.getBytes("ISO8859-1"),"UTF-8");
			queryfields = URLDecoder.decode(queryfields,"UTF-8");
			param.setQueryfields(queryfields);
		}
		//字段操作
		String queryoperates = request.getParameter("queryoperates");
		if(queryoperates != null) {
			queryoperates = new String(queryoperates.getBytes("ISO8859-1"),"UTF-8");
			queryoperates = URLDecoder.decode(queryoperates,"UTF-8");
			param.setQueryoperates(queryoperates);
		}		
		//查询关键字
		String keywords = request.getParameter("keywords");
		if(keywords != null) {
			keywords = new String(keywords.getBytes("ISO8859-1"),"UTF-8");
			keywords = URLDecoder.decode(keywords,"UTF-8");
			param.setKeywords(keywords);
		}
		//多个操作之间的关系
		String queryrelations = request.getParameter("queryrelations");
		if(queryrelations != null) {
			queryrelations = new String(queryrelations.getBytes("ISO8859-1"),"UTF-8");
			queryrelations = URLDecoder.decode(queryrelations,"UTF-8");
			param.setQueryrelations(queryrelations);
		}		
		//用户指定的查询范围
		String bbox = request.getParameter("bbox");
		if(bbox != null) {
			bbox = new String(bbox.getBytes("ISO8859-1"),"UTF-8");
			bbox = URLDecoder.decode(bbox,"UTF-8");
			param.setBbox(bbox);
		}
		//空间字段
		String geofield = request.getParameter("geofield");
		if(geofield != null) {
			geofield = new String(geofield.getBytes("ISO8859-1"),"UTF-8");
			geofield = URLDecoder.decode(geofield,"UTF-8");
			param.setGeofield(geofield);
		}
		//空间操作
		String geoaction = request.getParameter("geoaction");
		param.setGeoaction(geoaction);
		//ID字段
		String idfield = request.getParameter("idfield");
		if(idfield != null) {
			idfield = new String(idfield.getBytes("ISO8859-1"),"UTF-8");
			idfield = URLDecoder.decode(idfield,"UTF-8");
			param.setIdfield(idfield);
		}		
		//自定义SQL
		String usersql = request.getParameter("usersql");
		if(usersql != null) {
			usersql = new String(usersql.getBytes("ISO8859-1"),"UTF-8");
			usersql = URLDecoder.decode(usersql,"UTF-8");
			param.setUsersql(usersql);
		}
		//求和字段
		String sumfield = request.getParameter("sumfield");
		if(sumfield != null) {
			sumfield = new String(sumfield.getBytes("ISO8859-1"),"UTF-8");
			sumfield = URLDecoder.decode(sumfield,"UTF-8");
			param.setSumfield(sumfield);
		}
		//分组字段
		String groupfield = request.getParameter("groupfield");
		if(groupfield != null) {
			groupfield = new String(groupfield.getBytes("ISO8859-1"),"UTF-8");
			groupfield = URLDecoder.decode(groupfield,"UTF-8");
			param.setGroupfield(groupfield);
		}
		//数据存储类型
		String type = request.getParameter("type");
		param.setType(type);
		//输出格式
		String out = request.getParameter("out");
		param.setOut(out);
		//访问权限
		String token = request.getParameter("token");
		param.setToken(token);
		//时间标签
		String time = request.getParameter("time");
		if(time != null) {
			time = new String(time.getBytes("ISO8859-1"),"UTF-8");
			time = URLDecoder.decode(time,"UTF-8");
			param.setTime(time);
		}
		
		
		DataControl dataControl = new DataControl(); 
		String result = dataControl.process(param);
		
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// 创建 GZIPOutputStream 对象
        GZIPOutputStream gzipOut = new GZIPOutputStream(bout);
        // 将响应的数据写到 Gzip 压缩流中
        gzipOut.write(result.getBytes("UTF-8")); 
        gzipOut.close(); // 将数据刷新到  bout 字节流数组
		
		//设置头
		response.setCharacterEncoding("UTF-8");
		response.setContentType("application/json;charset=utf-8");
		response.setHeader("Access-Control-Allow-Origin", "*");
		
		//response.getWriter().write(result);
		//设置GZIP压缩
		response.setHeader("Content-Encoding", "gzip");
		
		response.getOutputStream().write(bout.toByteArray());
		response.getOutputStream().flush();
		response.getOutputStream().close();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
