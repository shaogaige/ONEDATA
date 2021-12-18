package com.idata.server;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
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

import com.google.gson.JsonArray;
import com.idata.core.OneDataServer;
import com.idata.core.SuperObject;
import com.idata.cotrol.TokenControl;
import com.idata.cotrol.VisitControl;
import com.idata.data.FileDriver;
import com.idata.tool.IPAddressUtil;
import com.idata.tool.LogUtil;
import com.idata.tool.POIDBToExcel;
import com.idata.tool.QRUtil;
import com.ojdbc.sql.Value;

/**
 * Servlet implementation class File
 */
@WebServlet(description = "文件单独上传下载接口", urlPatterns = { "/File" })
public class File extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
//    public File() {
//        super();
//        // TODO Auto-generated constructor stub
//    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		//数据操作
		String operation = request.getParameter("operation");
		//项目名称
		String project = request.getParameter("project");
		//访问权限
		String token = request.getParameter("token");
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
		String result = "";
		String filename = request.getParameter("filenames");
		if("process".equalsIgnoreCase(operation))
		{
			if (filename != null)
			{
				filename = new String(filename.getBytes("ISO8859-1"), "UTF-8");
				filename = URLDecoder.decode(filename, "UTF-8");
			}
			String fid = filename;
			//System.out.println(fid);
			String fstate = OneDataServer.filestate.getObject(fid);
			if(fstate == null)
			{
				SuperObject so = new SuperObject();
				so.addProperty("filename", new Value().setString_value(filename));
				so.addProperty("progress", new Value().setString_value("0"));
				so.addProperty("state", new Value().setString_value("未上传..."));
				fstate = so.getJSONString("json", null);
			}
			//输出
			result = fstate;
		}
		else if("add".equalsIgnoreCase(operation) || "delete".equalsIgnoreCase(operation))
		{
			String fid = filename;
			LogUtil.info("文件："+fid+"预处理中...");
			SuperObject so = new SuperObject();
			so.addProperty("filename", new Value().setString_value(filename));
			so.addProperty("progress", new Value().setString_value("1"));
			so.addProperty("state", new Value().setString_value("上传中"));
			so.addProperty("starttime", new Value().setString_value(OneDataServer.getCurrentTime()));
			
			OneDataServer.filestate.add(fid, so.getJSONString("json", null));
			
			// 使用Apache文件上传组件处理文件上传步骤：
			// 1、创建一个DiskFileItemFactory工厂
			DiskFileItemFactory factory = new DiskFileItemFactory();
			// 2、创建一个文件上传解析器
			ServletFileUpload upload = new ServletFileUpload(factory);
			// 解决上传文件名的中文乱码
			upload.setHeaderEncoding("UTF-8");
			// 3、判断提交上来的数据是否是上传表单的数据
			if (!ServletFileUpload.isMultipartContent(request))
			{
				result = "{\"state\":true,\"message\":\"文件上传格式不是Multipart！\",\"size\":0,\"data\":[]}";
			}
			else
			{
				 // 4、使用ServletFileUpload解析器解析上传数据，解析结果返回的是一个List<FileItem>集合
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
                try 
                {
					Map<String, List<FileItem>> map = upload.parseParameterMap(request);
					JsonArray jsonarr = new JsonArray();
					for (String key : map.keySet())
					{
						List<FileItem> list = map.get(key);
						for (FileItem item : list)
						{
							// 如果fileitem中封装的是普通输入项的数据
							if (item.isFormField())
							{
								result = "{\"state\":false,\"message\":\"文件上传失败,上传格式存在问题！\",\"size\":0,\"data\":[]}";
							}
							else
							{
								// 如果fileitem中封装的是上传文件
								// 得到上传的文件名称，
								filename = item.getName();
								//System.out.println("...接收到文件:"+filename);
								LogUtil.info("...接收到文件:"+filename+","+item.getSize()+" byte");
								if (filename == null || filename.trim().equals(""))
								{
									continue;
								}
								//记录进度
								long progress = 0;
								// 读取文件
								InputStream in = item.getInputStream();
								FileDriver fdriver = new FileDriver(); 
								
								if(item.getSize()>1024*1024*100)
								{
									jsonarr.add(fdriver.add(filename, in));
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
										//更新文件状态
										progress += size;
										so.addProperty("progress", new Value().setString_value(1.00*progress/item.getSize()*100 +""));
									}
									in.close();
									byte[] content = _out.toByteArray();
									//调用控制器
									jsonarr.add(fdriver.add(filename,content));
								}
								
								//更新文件状态
								//OneDataServer.filestate.remove(fid);
							}
						}
					}
					
					//输出
					result = "{\"state\":true,\"message\":\"文件上传成功\",\"size\":"+map.size()+",\"data\":"+jsonarr.toString()+"}";
					
				} catch (FileUploadException e) {
					// TODO Auto-generated catch block
					LogUtil.error(e);
				}
			}
		}
		else if("down".equalsIgnoreCase(operation))
		{
			//唯一标识
			String uid = request.getParameter("uid");
			//数据库连接字符串
			String con = request.getParameter("con");
			String field = request.getParameter("field");
			String operator = request.getParameter("operator");
			if (operator != null)
			{
				operator = new String(operator.getBytes("ISO8859-1"), "UTF-8");
				operator = URLDecoder.decode(operator, "UTF-8");
			}
			else
			{
				operator = "";
			}
			String value = request.getParameter("value");
			if (value != null)
			{
				value = new String(value.getBytes("ISO8859-1"), "UTF-8");
				value = URLDecoder.decode(value, "UTF-8");
			}
			String tableName = request.getParameter("layer");
			String fileName = tableName;
			String path = null;
			if (tableName != null && !"".equalsIgnoreCase(tableName) && fileName != null && !"".equalsIgnoreCase(fileName))
			{
				path = POIDBToExcel.DB2Excel(uid,con,tableName, fileName, field, value,operator);
				if (path != null)
				{
					java.io.File file = new java.io.File(path);
					if (file.exists() && file.isFile())
					{
						/* 第二步：根据已存在的文件，创建文件输入流 */
						InputStream inputStream = new FileInputStream(file);
						/* 第三步：创建缓冲区，大小为流的最大字符数 */
						byte[] buffer = new byte[1024 * 1024];
						/* 第四步：从文件输入流读字节流到缓冲区 */
						inputStream.read(buffer);
						fileName = file.getName();
						fileName = URLEncoder.encode(fileName, "UTF-8");
						response.reset();
						response.setCharacterEncoding("UTF-8");
						response.addHeader("Content-Disposition", "attachment;filename=" + fileName + ";charset=UTF-8");
						response.addHeader("Content-Length", "" + file.length());
						response.setHeader("Access-Control-Allow-Origin", "*");
						response.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE");    
				        response.setHeader("Access-Control-Allow-Headers", "x-requested-with");
						//System.out.println("下载文件大小:"+file.length()+" byte");
						LogUtil.info("下载文件:"+fileName+",大小:"+file.length()+" byte");
						/* 第六步：创建文件输出流 */
						OutputStream outputStream = response.getOutputStream();
						int len = 0;
						while ((len = inputStream.read(buffer)) != -1)
						{
							outputStream.write(buffer, 0, len);
						}
						response.setContentType("application/force-download");
						/* 第七步：把缓冲区的内容写入文件输出流 */
						outputStream.write(buffer);
						/* 第八步：刷空输出流，并输出所有被缓存的字节 */
						outputStream.flush();
						/* 第九步：关闭输出流 */
						outputStream.close();
						/* 第五步： 关闭输入流 */
						inputStream.close();
						
						return;

					}
				}

			}
		}
		else if("qr".equalsIgnoreCase(operation))
		{
			String url = request.getParameter("url");
			if (url != null)
			{
				url = new String(url.getBytes("ISO8859-1"), "UTF-8");
				url = URLDecoder.decode(url, "UTF-8");
			}
			byte[] rs =null;
			rs = QRUtil.getQRimage(url,"png");
			//设置头
			response.setCharacterEncoding("UTF-8");
			response.setContentType("image/png;charset=utf-8");
			response.setHeader("Access-Control-Allow-Origin", "*");
			
			response.getOutputStream().write(rs);
			response.getOutputStream().flush();
			response.getOutputStream().close();
			return;
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
			//log
			VisitControl.VisitLogModel visit = new VisitControl.VisitLogModel();
			visit.setReq_ip(IPAddressUtil.getClientIPAddress(request));
			visit.setReq_databaseid("0");
			visit.setReq_databasename("onedata_database");
			visit.setReq_interfacename("File");
			visit.setReq_interfacetype(operation);
			visit.setReq_project(project);
			visit.setReq_layer(filename);
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
