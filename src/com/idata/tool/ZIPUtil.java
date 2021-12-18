/**
 * ClassName:ZIPUtil.java
 * Date:2021年12月8日
 */
package com.idata.tool;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Creater: ShaoGaige
 * Description:ZIP压缩解压类
 * Log:
 */
public class ZIPUtil {
	
	/**
	 * zip压缩
	 * @param filePaths
	 * @param zipFilePath
	 * @return boolean
	 */
	public static boolean zip(List<String> filePaths,String zipFilePath)
	{
		if(filePaths == null || filePaths.size()<1)
		{
			return false;
		}
		File zipFile = null;
		if(zipFilePath == null || "".equalsIgnoreCase(zipFilePath))
		{
			//默认第一个文件的同级目录
			String f = filePaths.get(0);
			f = f.substring(0, f.lastIndexOf(".")+1);
			f += "zip";
			zipFile = new File(f);
		}
		else
		{
			zipFile = new File(zipFilePath);
			if(!zipFilePath.endsWith(".zip") && zipFile.isDirectory())
			{
				zipFile = new File(zipFilePath+"zipfile.zip");
			}
		}
		//文件打包操作
        ZipOutputStream zipStream = null;
        FileInputStream zipSource = null;
        BufferedInputStream bufferStream = null;
        try 
        {
            zipStream = new ZipOutputStream(new FileOutputStream(zipFile));// 用这个构造最终压缩包的输出流
            //zipSource = null;// 将源头文件格式化为输入流
            
            for (String picKey : filePaths) 
            {
                
                File file = new File(picKey);
                zipSource = new FileInputStream(file);

                byte[] bufferArea = new byte[1024 * 10];// 读写缓冲区

                // 压缩条目不是具体独立的文件，而是压缩包文件列表中的列表项，称为条目，就像索引一样
                ZipEntry zipEntry = new ZipEntry(file.getName());
                zipStream.putNextEntry(zipEntry);// 定位到该压缩条目位置，开始写入文件到压缩包中

                bufferStream = new BufferedInputStream(zipSource, 1024 * 10);// 输入缓冲流
                int read = 0;

                // 在任何情况下，b[0] 到 b[off] 的元素以及 b[off+len] 到 b[b.length-1]
                // 的元素都不会受到影响。这个是官方API给出的read方法说明，经典！
                while ((read = bufferStream.read(bufferArea, 0, 1024 * 10)) != -1) 
                {
                    zipStream.write(bufferArea, 0, read);
                }
            }
            System.out.println("******************ZIP压缩完毕********************");
            return true;
        } 
        catch (Exception e) 
        {
        	e.printStackTrace();
            // TODO: handle exception
            return false;
        } 
        finally 
        {
            // 关闭流
            try {
                if (null != bufferStream)
                    bufferStream.close();
                if (null != zipStream)
                    zipStream.close();
                if (null != zipSource)
                    zipSource.close();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                //e.printStackTrace();
                return false;
            }
        }
	}
	
	/**
	 * zip解压
	 * @param zipPath
	 * @param descDir
	 * @return boolean
	 */
	public static boolean unzip(String zipPath,String descDir)
	{
		if(zipPath == null || "".equalsIgnoreCase(zipPath))
		{
			return false;
		}
		File zipFile = new File(zipPath);
		if(descDir == null || "".equalsIgnoreCase(descDir))
		{
			descDir = zipFile.getParent();
		}
		File pathFile = new File(descDir);
	    if(!pathFile.exists())
	    {
	      pathFile.mkdirs();
	    }
	    InputStream is = null;
	    FileOutputStream fos = null;
	    ZipFile zip = null;
	    try
	    {
	    	//解决zip文件中有中文目录或者中文文件
		    zip = new ZipFile(zipFile, Charset.forName("GBK"));
		    //开始解压
	        Enumeration<?> entries = zip.entries();
	        while (entries.hasMoreElements()) 
	        {
	            ZipEntry entry = (ZipEntry) entries.nextElement();
	            // 如果是文件夹，就创建个文件夹
	            if (entry.isDirectory()) {
	            	zipFile.mkdirs();
	            } else {
	                // 如果是文件，就先创建一个文件，然后用io流把内容copy过去
	                File targetFile = new File(descDir + "/" + entry.getName());
	                // 保证这个文件的父文件夹必须要存在
	                if (!targetFile.getParentFile().exists()) {
	                    targetFile.getParentFile().mkdirs();
	                }
	                targetFile.createNewFile();
	                // 将压缩文件内容写入到这个文件中
	                is = zip.getInputStream(entry);
	                fos = new FileOutputStream(targetFile);
	                int len;
	                byte[] buf = new byte[1024*1024];
	                while ((len = is.read(buf)) != -1) {
	                    fos.write(buf, 0, len);
	                }
	            }
	        }
		    System.out.println("******************ZIP解压完毕********************");
			return true;
	    }
	    catch (Exception e) 
        {
	    	e.printStackTrace();
            // TODO: handle exception
            return false;
        } 
	    finally
	    {
	    	// 关闭流
            try {
                if (null != fos)
                	fos.close();
                if (null != is)
                	is.close();
                if (null != zip)
                	zip.close();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                //e.printStackTrace();
                return false;
            }
	    }
	}

}
