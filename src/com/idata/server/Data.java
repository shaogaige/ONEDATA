package com.idata.server;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.idata.core.DataParam;

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
		//唯一标识
		String id = request.getParameter("id");
		param.setId(id);
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
		//字段名
		String field = request.getParameter("field");
		if(field != null) {
			field = new String(field.getBytes("ISO8859-1"),"UTF-8");
			field = URLDecoder.decode(field,"UTF-8");
			param.setField(field);
		}
		//查询关键字
		String keyword = request.getParameter("keyword");
		if(keyword != null) {
			keyword = new String(keyword.getBytes("ISO8859-1"),"UTF-8");
			keyword = URLDecoder.decode(keyword,"UTF-8");
			param.setKeyword(keyword);
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
		
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
