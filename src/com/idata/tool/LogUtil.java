/**
 * ClassName:LogUtil.java
 * Date:2018年5月30日
 */
package com.idata.tool;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Creater:SHAO Gaige
 * Description:日志记录工具
 * Log:
 */
public class LogUtil {
	
	private static final int MESSAGE_MAX_LENGTH = 1024*2;
	
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static Level outLevel = Level.DEBUG;
	
	private static boolean printToConsole = true;
	
	private static boolean printToFile = true;
	
	private static File logFile;
	
	private static RandomAccessFile logFileStream;
	
	public static synchronized boolean setLogFile(String fileName)
	{
		try
		{
			String path = LogUtil.class.getClassLoader().getResource("").getPath();
			System.out.println("log file path:"+path+fileName);
			File file = new File(path+fileName);
			return setLogFile(file);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	public static synchronized boolean setLogFile(File file)
	{
		logFile = file;
		File pFile = new File(file.getParent());
		if (!pFile.exists()) {
			pFile.mkdirs();
		}
		if(logFileStream != null)
		{
			closeStream(logFileStream);
			logFileStream = null;
		}
		if(logFile != null)
		{
			try {
				logFileStream = new RandomAccessFile(logFile, "rw");
                logFileStream.seek(logFile.length());
            } catch (IOException e) {
                closeStream(logFileStream);
                logFileStream = null;
                return false;
            }
			return true;
		}
		return false;
	}
	
	private static boolean closeStream(Closeable stream)
	{
		if (stream != null) {
            try {
                stream.close();
            } catch (Exception e) {
                return false;
            }
        }
		return true;
	}
	
	public static void setPrintTarget(boolean console,boolean file)
	{
		printToConsole = console;
		printToFile = file;
	}
	
	public static void debug(String message) {
        printLog(Level.DEBUG, message);
    }

    public static void info(String message) {
        printLog(Level.INFO, message);
    }

    public static void warn(String message) {
        printLog(Level.WARN, message);
    }

    public static void error(String message) {
        printLog(Level.ERROR, message);
    }

    public static void error(Exception e) {
        if (e == null) {
            error((String) null);
            return;
        }

        PrintStream printOut = null;

        try {
            ByteArrayOutputStream bytesBufOut = new ByteArrayOutputStream();
            printOut = new PrintStream(bytesBufOut);
            e.printStackTrace(printOut);
            printOut.flush();
            error(new String(bytesBufOut.toByteArray(), "UTF-8"));

        } catch (Exception e1) {
            e1.printStackTrace();

        } finally {
            closeStream(printOut);
        }
    }

    private static void printLog(Level level, String message) {
        if (level.getLevelValue() >= outLevel.getLevelValue()) {
            String log = dateFormat.format(new Date()) +" " +level.getTag() +": " +
                    checkTextLengthLimit(message, MESSAGE_MAX_LENGTH)+"\r\n";
            
            if (printToConsole) {
            	if(level==Level.ERROR)
            	{
            		outLogToConsole(true, log);
            	}
            	else
            	{
            		outLogToConsole(false, log);
            	}
                
            }
            if (printToFile) {
                outLogToFile(log);
            }
        }
    }

    private static void outLogToConsole(boolean isOutToErr, String log) {
        if (isOutToErr) {
            System.err.println(log);
        } else {
            System.out.println(log);
        }
    }

    private static synchronized void outLogToFile(String log) {
        if (logFileStream != null) {
            try {
                logFileStream.write((log + "\n").getBytes("UTF-8"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static String checkTextLengthLimit(String text, int maxLength) {
        if ((text != null) && (text.length() >  maxLength)) {
            text = text.substring(0, maxLength - 3) + "...";
        }
        return text;
    }
			
	//----------内部静态枚举类------------		
    public static enum Level {
		
        DEBUG("Debug", 1), INFO("Info", 2), WARN("Warn", 3), ERROR("Error", 4);

        private String tag;

        private int levelValue;

        private Level(String tag, int levelValue) {
            this.tag = tag;
            this.levelValue = levelValue;
        }

        public String getTag() {
            return tag;
        }

        public int getLevelValue() {
            return levelValue;
        }
    }

}
