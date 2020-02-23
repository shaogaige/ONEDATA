/**
 * ClassName:ClassUtil
 * Description:找到某路径下，某接口或者类的子类
 * Creater:邵改革
 * Date：20160831
 * 
 */
package com.idata.tool;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ClassUtil {
	
	private static boolean isFunction(Class<?> c,Class<?> oc) {  
	    if (c == null) {  
	        return false;  
	    }  
	    if (c.isInterface()) {  
	        return false;  
	    }  
	    if (Modifier.isAbstract(c.getModifiers())) {  
	        return false;// 抽象  
	    }  
	    if(c.equals(oc))
	    {
	    	return false;  
	    }
	    return oc.isAssignableFrom(c); 
	    
	}  
	
	private static List<File> listPaths(Class<?> oc) {  
	    List<File> files = new ArrayList<File>();  
	    String jars = System.getProperty("java.class.path");  
	    if (jars == null) {  
	        System.err.println("java.class.path is null!");  
	        return files;  
	    }  
	    URL root = ClassUtil.class.getClassLoader().getResource("");  
	    if (root == null) {  
	        System.err.println("path root is null!");  
	        return files;  
	    }  
	    String path = null;  
	    try {  
	        path = URLDecoder.decode(root.getFile(), "UTF-8");  
	    } catch (UnsupportedEncodingException e) {  
	        e.printStackTrace();  
	        return files;  
	    }  
	    File dir = new File(path);  
	    String[] array = (jars).split(";");  
	    if (array != null) {  
	        for (String s : array) {  
	            if (s == null) {  
	                continue;  
	            }  
	            File f = new File(s);  
	            if (f.exists()) {  
	                files.add(f);  
	            } else {//有些jar就在系统目录下,省略了路径,要加上  
	                File jar = new File(dir, s);  
	                if (jar.exists()) {  
	                    files.add(jar);  
	                }  
	            }  
	        }  
	    }
	    //web,tomcat环境下 20160909修改 shaogaige
	    String applicationPath = getAPPPath(oc);
	    files.addAll(getAPPJar(applicationPath));
	    return files;  
	}  
	
	
	public static List<Class<?>> getClasses(String pkg,Class<?> oc) {  
	    List<Class<?>> list = new ArrayList<Class<?>>();  
	    for (File f : ClassUtil.listPaths(oc)) {  
	        // 如果是以文件的形式保存在服务器上  
	        if (f.isDirectory()) {  
	            // 获取包的物理路径  
	            String path = pkg.replace('.', File.separatorChar);  
	            ClassUtil.dirWalker(path, f, list,oc);  
	        } else {//尝试是否是jar文件  
	            // 获取jar  
	            JarFile jar = null;  
	            try {  
	                jar = new JarFile(f);  
	            } catch (IOException e) {  
	                // 有可能不是一个jar  
	            }  
	            if (jar == null) {  
	                continue;  
	            }  
	            String path = pkg.replace('.', '/');  
	            // 从此jar包 得到一个枚举类  
	            Enumeration<JarEntry> entries = jar.entries();  
	            // 同样的进行循环迭代  
	            while (entries.hasMoreElements()) {  
	                // 获取jar里的一个实体 可以是目录 和一些jar包里的其他文件 如META-INF等文件  
	                JarEntry entry = entries.nextElement();  
	                String name = entry.getName();  
	                // 如果是以/开头的  
	                if (name.charAt(0) == '/') {  
	                    // 获取后面的字符串  
	                    name = name.substring(1);  
	                }  
	                // 如果前半部分和定义的包名相同  
	                if (name.contains(path)) {  
	                    if (name.endsWith(".class") && !entry.isDirectory()) {  
	                        name = name.replace("/", ".").substring(0,  
	                                name.lastIndexOf("."));  
	                        try {  
	                            Class<?> c = Class.forName(name);  
	                            if (ClassUtil.isFunction(c,oc)) {  
	                                list.add(c);  
	                            }  
	                        } catch (Exception e) {  
	                            // 找不到无所谓了  
	                        }  
	                    }  
	                }  
	            }  
	        }  
	    } 
	    //去重
	    HashSet<Class<?>> set = new HashSet<Class<?>>(list);
        list.clear();
        list.addAll(set);
	    return list;  
	}  
	
	
	private static void dirWalker(String path, File file, List<Class<?>> list,Class<?> oc) {  
	    if (file.exists()) {  
	        if (file.isDirectory()) {  
	            for (File f : file.listFiles()) {  
	            	ClassUtil.dirWalker(path, f, list,oc);  
	            }  
	        } else {  
	            Class<?> c = ClassUtil.loadClassByFile(path, file,oc);  
	            if (c != null) {  
	                list.add(c);  
	            }  
	        }  
	    }  
	} 
	
	
	private static Class<?> loadClassByFile(String pkg, File file,Class<?> oc) {  
	    if (!file.isFile()) {  
	        return null;  
	    }  
	    String name = file.getName();  
	    if (name.endsWith(".class")) {  
	        String ap = file.getAbsolutePath();  
	        if (!ap.contains(pkg)) {  
	            return null;  
	        }  
	        name = ap.substring(ap.indexOf(pkg) + pkg.length());  
	        if (name.startsWith(File.separator)) {  
	            name = name.substring(1);  
	        }  
	        String path = (pkg + "." + name.substring(0, name.lastIndexOf(".")))  
	                .replace(File.separatorChar, '.');  
	        try {  
	            Class<?> c = Class.forName(path);  
	            if (ClassUtil.isFunction(c,oc)) {  
	                return c;  
	            }  
	        } catch (ClassNotFoundException e) {  
	            // do nothing  
	        }  
	    }  
	    return null;  
	}
	
	private static String getAPPPath(Class<?> cls){ 
	       //检查用户传入的参数是否为空 
        if (cls==null )  
        throw new java.lang.IllegalArgumentException("参数不能为空！"); 
 
        ClassLoader loader=cls.getClassLoader(); 
        //获得类的全名，包括包名 
        String clsName=cls.getName();
        //此处简单判定是否是Java基础类库，防止用户传入JDK内置的类库 
        if (clsName.startsWith("java.")||clsName.startsWith("javax.")) {
        throw new java.lang.IllegalArgumentException("不要传送系统类！");
        }
        //将类的class文件全名改为路径形式
        String clsPath= clsName.replace(".", "/")+".class"; 
 
        //调用ClassLoader的getResource方法，传入包含路径信息的类文件名 
        java.net.URL url =loader.getResource(clsPath); 
        //从URL对象中获取路径信息 
        String realPath=url.getPath(); 
        //去掉路径信息中的协议名"file:" 
        int pos=realPath.indexOf("file:"); 
        if (pos>-1) {
        realPath=realPath.substring(pos+5); 
        }
        //去掉路径信息最后包含类文件信息的部分，得到类所在的路径 
        pos=realPath.indexOf(clsPath); 
        realPath=realPath.substring(0,pos-1); 
        //如果类文件被打包到JAR等文件中时，去掉对应的JAR等打包文件名 
        if (realPath.endsWith("!")) {
        realPath=realPath.substring(0,realPath.lastIndexOf("/"));
        }
        java.io.File file = new java.io.File(realPath);
        realPath = file.getAbsolutePath();
 
        try { 
        realPath=java.net.URLDecoder.decode (realPath,"utf-8"); 
        }catch (Exception e){
        throw new RuntimeException(e);
        } 
        return realPath; 
    }//getAppPath定义结束 
	
	private static List<File> getAPPJar(String appPath)
	{
		List<File> files = new ArrayList<File>();
		File newdir = new File(appPath);
		    File[] all = newdir.listFiles();
		    for(File f:all)
		    {
		    	if(f.exists())
		    	{
		    		files.add(f); 
		    	}
		    }
		return files;
	}
}
