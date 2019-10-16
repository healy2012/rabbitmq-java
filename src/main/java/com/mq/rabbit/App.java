package com.mq.rabbit;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * 程序入口
 * 
 * @author stone
 *
 */
public class App {

	public static void main(String[] args) throws Exception {
		// 1、direct 交换器实现 => 请运行DirctExchange.java
		// TODO:其他路由模式实现
//		directExchange.publisher();
			
//		new App().directExchage();
//		new App().fanoutExchange();	
//		new App().topicExchage();
		
//		new App().confirmMessage();
		
		new App().transaction();
		
	}
	
	public void transaction() throws KeyManagementException, NoSuchAlgorithmException, URISyntaxException, IOException, TimeoutException, Exception {
		transactionExample.publish();
		transactionExample.consume();
	}
	
	public void confirmMessage() throws IOException, TimeoutException, InterruptedException {
		confirmExample.publish();
		confirmExample.publishConfirmOneByOne();
		confirmExample.publishConfirmBatch();
		confirmExample.publishConfirmAsync();
		directExchange.consumerBatch("消费者1");
	}
	
	public void topicExchage() throws IOException, TimeoutException {
		Thread t1 = new Thread(() -> {
			try {
				topicExchange.consumer("com.rabbit.*");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		Thread t2 = new Thread(() -> {
			try {
				topicExchange.consumer("com.#");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		Thread t3 = new Thread(() -> {
			try {			
				Thread.sleep(200);//延迟200毫秒连接
				topicExchange.consumer("*.rabbit.*");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		t1.start();
		t2.start();
		t3.start();
		
		String topic = "com.rabbit.logic";
		for(int i=0;i<20;i++) {
			topicExchange.publisher(topic);
		}
	}
	
	public void fanoutExchange() throws IOException, TimeoutException {
		Thread t1 = new Thread(() -> {
			try {
				fanoutExchange.consumer("消费者1");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		Thread t2 = new Thread(() -> {
			try {
				fanoutExchange.consumer("消费者2");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		Thread t3 = new Thread(() -> {
			try {			
				Thread.sleep(200);//延迟200毫秒连接
				fanoutExchange.consumer("消费者3");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		t1.start();
		t2.start();
		t3.start();
				
		for(int i=0;i<20;i++) {
			fanoutExchange.publisher();
		}
	}
	
	/**
	 * 同一个队列的多个消费者，公平获得队列消息，没有ack的消息下次连接会再次获取
	 * @throws IOException
	 * @throws TimeoutException
	 */
	public void directExchage() throws IOException, TimeoutException {
		Thread t1 = new Thread(() -> {
			try {
				directExchange.consumer("消费者1");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		Thread t2 = new Thread(() -> {
			try {
				directExchange.consumer("消费者2");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		Thread t3 = new Thread(() -> {
			try {			
				Thread.sleep(200);//延迟200毫秒连接
				directExchange.consumer("消费者3");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		t1.start();
		t2.start();		
		t3.start();
				
		for(int i=0;i<20;i++) {
			directExchange.publisher();
		}
	}

}
