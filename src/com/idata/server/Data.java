package com.idata.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.tomcat.util.http.fileupload.FileItem;
import org.apache.tomcat.util.http.fileupload.FileUploadException;
import org.apache.tomcat.util.http.fileupload.ProgressListener;
import org.apache.tomcat.util.http.fileupload.disk.DiskFileItemFactory;
import org.apache.tomcat.util.http.fileupload.servlet.ServletFileUpload;
import org.apache.tomcat.util.http.fileupload.servlet.ServletRequestContext;

import com.idata.core.DataParam;
import com.idata.core.OneDataServer;
import com.idata.core.SuperObject;
import com.idata.cotrol.DataControl;
import com.idata.cotrol.TokenControl;
import com.idata.cotrol.VisitControl;
import com.idata.data.FileDriver;
import com.idata.tool.IPAddressUtil;
import com.idata.tool.LogUtil;
import com.ojdbc.sql.Value;

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
		if(con != null)
		{
			param.setCon(con);
		}
		//项目名称
		String project = request.getParameter("project");
		param.setProjectname(project);
		//图层名，表名
		String layer = request.getParameter("layer");
		if(layer != null) {
			layer = new String(layer.getBytes("ISO8859-1"),"UTF-8");
			layer = URLDecoder.decode(layer,"UTF-8");
			param.setLayer(layer);
		}
		//数据操作
		String operation = request.getParameter("operation");
		if(operation != null)
		{
			param.setOperation(operation);
		}
		//分页
		String start = request.getParameter("start");
		if(start != null)
		{
			param.setStart_s(start);
		}
		String cout = request.getParameter("count");
		if(cout != null)
		{
			param.setCount_s(cout);
		}
		
		//实体数据
		String jsondata = request.getParameter("jsondata");
		//System.out.println("jsondata:"+jsondata);
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
		String queryvalues = request.getParameter("queryvalues");
		if(queryvalues != null) {
			queryvalues = new String(queryvalues.getBytes("ISO8859-1"),"UTF-8");
			queryvalues = URLDecoder.decode(queryvalues,"UTF-8");
			param.setQueryvalues(queryvalues);
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
		if(geoaction != null && !"".equalsIgnoreCase(geoaction))
		{
			param.setGeoaction(geoaction);
		}
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
		//排序字段
		String orderfields = request.getParameter("orderfields");
		if(orderfields != null) {
			orderfields = new String(orderfields.getBytes("ISO8859-1"),"UTF-8");
			orderfields = URLDecoder.decode(orderfields,"UTF-8");
			param.setOrderfields(orderfields);
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
		//文件名称
		String filenames = request.getParameter("filenames");
		SuperObject so = new SuperObject();
		String fid = filenames;
		if(filenames != null) {
			filenames = new String(filenames.getBytes("ISO8859-1"),"UTF-8");
			filenames = URLDecoder.decode(filenames,"UTF-8");
			param.setFilenames(filenames);
			
			//记录进度
			//String filenames = dataControl.getFileNames(param);
			LogUtil.info("文件："+fid+"上传中...");
			so.addProperty("filename", new Value().setString_value(filenames));
			so.addProperty("progress", new Value().setString_value("2"));
			so.addProperty("state", new Value().setString_value("上传中"));
			so.addProperty("starttime", new Value().setString_value(OneDataServer.getCurrentTime()));
			
			OneDataServer.filestate.add(fid, so.getJSONString("json", null));
		}
		else
		{
			filenames = "";
		}
		//数据存储类型
		String type = request.getParameter("type");
		if(type != null && !"".equalsIgnoreCase(type))
		{
			param.setType(type);
		}
		//输出格式
		String out = request.getParameter("out");
		if(out != null && !"".equalsIgnoreCase(out))
		{
			param.setOut(out);
		}
		//访问权限
		String token = request.getParameter("token");
		if(token != null)
		{
			param.setToken(token);
		}
	    //token验证
		if(OneDataServer.checkToken)
		{
			TokenControl tokencontrol = new TokenControl();
			boolean f = tokencontrol.check(token);
			if(!f)
			{
				//设置头
				response.setCharacterEncoding("UTF-8");
				response.setContentType("application/json;charset=utf-8");
				response.setHeader("Access-Control-Allow-Origin", "*");
				//参数有错误
				response.getWriter().write("{\"state\":false,\"message\":\"非法访问用户,token不存在或已过期,请联系管理员！\",\"size\":0,\"data\":[]}");
				return;
			}
		}
		//时间标签
		String time = request.getParameter("time");
		if(time != null) {
			time = new String(time.getBytes("ISO8859-1"),"UTF-8");
			time = URLDecoder.decode(time,"UTF-8");
			param.setTime(time);
		}
		//文件处理
		if("add".equalsIgnoreCase(operation) || "edit".equalsIgnoreCase(operation))
		{
			// 使用Apache文件上传组件处理文件上传步骤：
			// 1、创建一个DiskFileItemFactory工厂
			DiskFileItemFactory factory = new DiskFileItemFactory();
			// 2、创建一个文件上传解析器
			ServletFileUpload upload = new ServletFileUpload(factory);
			// 解决上传文件名的中文乱码
			upload.setHeaderEncoding("UTF-8");
			
			if(fid != null && !"".equalsIgnoreCase(fid))
			{
				upload.setProgressListener(new ProgressListener() {
					@Override
					public void update(long arg0, long arg1, int arg2) {
						// TODO Auto-generated method stub
						//System.out.println("文件大小为："+arg1+",当前已处理："+arg0);
						so.addProperty("size", new Value().setString_value(arg1+" byte"));
						//更新文件状态
						so.addProperty("progress", new Value().setString_value((int)Math.floor(1.00*arg0/arg1*100)+""));
						OneDataServer.filestate.add(fid, so.getJSONString("json", null));
						//System.out.println(OneDataServer.filestate.get(fid).getProperty("progress").getString_value());
					}
				});
			}
			
			// 3、判断提交上来的数据是否是上传表单的数据
			boolean multipart = ServletFileUpload.isMultipartContent(request);
			if (!multipart && (jsondata==null || "".equalsIgnoreCase(jsondata)))
			{
				// 按照传统方式获取数据
				String data = request.getParameter("jsondata");
				if (data != null)
				{
					data = new String(data.getBytes("ISO8859-1"), "UTF-8");
					data = URLDecoder.decode(data, "UTF-8");
				}
				//System.out.println("jsondata2:"+data);
				param.setJsondata(data);
				// return;
			}
			else if(multipart)
			{
				// 4、使用ServletFileUpload解析器解析上传数据，解析结果返回的是一个List<FileItem>集合
				try 
				{
					List<FileItem> list = upload.parseRequest(new ServletRequestContext(request));
					for (FileItem item : list)
					{
						// 如果fileitem中封装的是普通输入项的数据
						if (item.isFormField())
						{
							if(jsondata==null || "".equalsIgnoreCase(jsondata))
							{
								//String name = item.getFieldName();
								// 解决普通输入项的数据的中文乱码问题
								String value = item.getString("UTF-8");
								value = new String(value.getBytes("ISO8859-1"), "UTF-8");
								value = URLDecoder.decode(value, "UTF-8");
								//System.out.println("jsondata3:"+value);
								param.setJsondata(value);
							}
						}
						else
						{
							// 如果fileitem中封装的是上传文件
							// 得到上传的文件名称，
							String filename = item.getName();
							LogUtil.info("...接收到文件:"+filename+","+item.getSize()+" byte");
							if (filename == null || filename.trim().equals(""))
							{
								continue;
							}
							// 读取文件
							InputStream in = item.getInputStream();
							if(item.getSize()>1024*1024*10)
							{
								FileDriver fd = new FileDriver();
								fd.add(param, in);
								in.close();
							}
							else
							{
								ByteArrayOutputStream _out = new ByteArrayOutputStream(1024 * 1024);
								byte[] temp = new byte[1024 * 1024];
								int size = 0;
								while ((size = in.read(temp)) != -1)
								{
									_out.write(temp, 0, size);
								}
								in.close();
								byte[] content = _out.toByteArray();
								param.addFile(filename, content);
							}
							
							
						}
					}
					
				} catch (FileUploadException e) {
					// TODO Auto-generated catch block
					LogUtil.error(e);
					VisitControl.log(param, e, LogUtil.Level.ERROR, "Data");
				}
			}
			
		}
		
		String result = "";
		DataControl dataControl = new DataControl(); 
		if("query".equalsIgnoreCase(operation) || "group".equalsIgnoreCase(operation))
		{
			String param_str = param.toString();
			if(OneDataServer.resultcache.containsKey(param_str))
			{
				result = OneDataServer.resultcache.getObject(param_str);
			}
			else
			{
				//调用控制器
				result = dataControl.process(param);
				OneDataServer.resultcache.add(param_str, result);
			}
		}
		else
		{
			//调用控制器
			result = dataControl.process(param);
		}
		
		
		//结果输出
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
		response.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE");  
        response.setHeader("Access-Control-Max-Age", "3600");  
        response.setHeader("Access-Control-Allow-Headers", "x-requested-with");
		
		//response.getWriter().write(result);
		//设置GZIP压缩
		response.setHeader("Content-Encoding", "gzip");
		
		response.getOutputStream().write(bout.toByteArray());
		response.getOutputStream().flush();
		response.getOutputStream().close();
		//end
		
		if(OneDataServer.visitlog)
		{
			//记录表访问次数
			DataParam dataParam = new DataParam();
			dataParam.setCon(OneDataServer.SQLITEDBHandle.getEncodeConStr());
			dataParam.setLayer(OneDataServer.TablePrefix+"datavisit");
			dataParam.setOperation("add");
	        dataParam.setJsonDataProperty("database_id", new Value().setString_value(uid));
	        dataParam.setJsonDataProperty("database_conname", OneDataServer.databaseinfo.getObject(uid).getProperty("database_conname"));
	        dataParam.setJsonDataProperty("database_type", OneDataServer.databaseinfo.getObject(uid).getProperty("database_type"));
	        dataParam.setJsonDataProperty("table_name", new Value().setString_value(layer));
	        dataParam.setJsonDataProperty("interface_id", new Value().setString_value("0"));
	        dataParam.setJsonDataProperty("interface_name", new Value().setString_value("Data"));//程序接口
	        dataParam.setJsonDataProperty("interface_type", new Value().setString_value(operation));
	        dataParam.setJsonDataProperty("operation", new Value().setString_value(operation));//访问接口
	        dataParam.setJsonDataProperty("visit_count", new Value().setInt_value(1));
	        dataParam.setJsonDataProperty("last_reqtime", new Value().setString_value(OneDataServer.getCurrentTime()));
	        dataParam.setJsonDataProperty("remark", new Value().setString_value(type));
			dataParam.setQueryfields("database_id,table_name,interface_name,operation");
			//dataParam.setQueryoperates("=");
			dataParam.setQueryvalues(uid+","+layer+",Data"+","+operation);
			VisitControl.log(dataParam,OneDataServer.visitlog);
			
			//log
			VisitControl.VisitLogModel visit = new VisitControl.VisitLogModel();
			visit.setReq_ip(IPAddressUtil.getClientIPAddress(request));
			visit.setReq_databaseid(uid);
			visit.setReq_databasename(OneDataServer.databaseinfo.getObject(uid).getProperty("database_conname").getString_value());
			visit.setReq_interfacename("Data");
			visit.setReq_interfacetype(operation);
			visit.setReq_project(project);
			visit.setReq_layer(layer);
			visit.setReq_keyword(request.getQueryString());
			visit.setResults(result);
			visit.setToken(token);
			VisitControl.log(visit);
		}
		
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
