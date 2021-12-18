/**
 * ClassName:ActiveMQUtil.java
 * Date:2021年3月13日
 */
package com.idata.tool;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

import com.idata.cotrol.VisitControl;

/**
 * Creater:SHAO Gaige
 * Description:ActiveMQ使用工具类
 * Log:
 */
public class ActiveMQUtil {
	
	//连接URL
	private static String brokeurl = "tcp://localhost:61616";
	//嵌入式的服务启动
	private static BrokerService brokerService = null;
	//消费者
	private static MessageConsumer messageConsumer = null;
	private static Session sessionConsumer = null;
	private static Connection connectionConsumer = null;
	//生产者
	private static MessageProducer messageProducer = null;
	private static Session sessionProducer = null;
	private static Connection connectionProducer = null;
	static 
	{
		try 
		{
			String url = PropertiesUtil.getValue("ACTIVEMQURL");
			if(url != null && !"".equalsIgnoreCase(url))
			{
				brokeurl = url;
			}
			BrokerService brokerService = new BrokerService();
			brokerService.setUseJmx(true);
			brokerService.addConnector(brokeurl);
			brokerService.start();
			
			doShutDownWork();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void initConsumer() throws Exception
	{
		//创建连接工厂
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokeurl);
        //创建连接
        connectionConsumer = activeMQConnectionFactory.createConnection();
        //连接
        connectionConsumer.start();
        //创建会话
        sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //创建队列
        Queue queue = sessionConsumer.createQueue("queue_onedata");
        //创建消息消费者
        messageConsumer = sessionConsumer.createConsumer(queue);
        // 消息消费，使用监听器
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage tmsg = (TextMessage) message;    
                try 
                {
                    //System.out.println("接收到数据:" + tmsg.getText());
                    //处理数据
                    VisitControl.save(tmsg.getText());
                } catch (JMSException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
	}
	
	
	public static void initProducer() throws Exception
	{
		//创建连接工厂
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokeurl);
        //创建连接
        connectionProducer = activeMQConnectionFactory.createConnection();
        //连接
        connectionProducer.start();
        //创建会话
        sessionProducer = connectionProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //创建队列
        Queue queue = sessionProducer.createQueue("queue_onedata");
        //创建消息生产者
        messageProducer = sessionProducer.createProducer(queue);
	}
	
	private static void doShutDownWork() 
	{
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() {
		     try 
		     {
		    	 if(messageConsumer != null)
		    	 {
		    		 messageConsumer.close();
		    	     sessionConsumer.close();
		    	     connectionConsumer.close();
		    	 }
		    	 if(messageProducer != null)
		    	 {
		    		//关闭所有连接
		    	    messageProducer.close();
		    	    sessionProducer.close();
		    	    connectionProducer.close();
		    	 }
		    	 if(brokerService != null)
		    	 {
		    		 brokerService.stop();
		    	 }
		     } 
		     catch (Exception ex) 
		     {
		    	 ex.printStackTrace();
		     }    
		    }  
		   });
	}
	
	public static boolean sendMessage(String msg) throws Exception
	{
		Message message = sessionProducer.createTextMessage(msg);
        //发送消息
        messageProducer.send(message);
		return true;
	}

}
