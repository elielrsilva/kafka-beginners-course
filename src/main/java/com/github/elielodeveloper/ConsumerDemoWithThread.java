package com.github.elielodeveloper;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {
	
	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}
	
	private ConsumerDemoWithThread() {
		
	}
	
	private void run(){
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		//settings values
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-sixth-application";
		String topic = "second_topic";
		CountDownLatch latch = new CountDownLatch(1);
		
		logger.info("Creating the consumer thread");
		Runnable myConsumerThread = new ConsumerThread(
			bootstrapServers,
			groupId,
			topic,
			latch
		);
		
		//start the thread
		Thread myThread = new Thread(myConsumerThread);
		myThread.start();
		
		//add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shutdown hook");
			((ConsumerThread) myConsumerThread).shutdown();
		}));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}
	
	public class ConsumerThread implements Runnable {

		private KafkaConsumer<String, String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		private CountDownLatch latch;
		
		public ConsumerThread(
				String bootstrapServers,
				String groupId,
				String topic,
				CountDownLatch latch
				){
			this.latch = latch;
			//create consumer config
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			//create consumer
			consumer = new KafkaConsumer <String, String>(properties);
			//subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic));
		}
		
		public void run() {
			//poll for new data
			try {
				while(true){
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
				
					for (ConsumerRecord<String, String> record : records){
						logger.info("Key: " + record.key() + ", Value: " + record.value());
						logger.info("Partition: " + record.partition() + ", Offset:"+ record.offset());
					}
				}
			} catch (WakeupException exception) {
				logger.info("Received shudown signal");
			} finally {
				consumer.close();
				latch.countDown();
			}
		}
		
		public void shutdown(){
			// the wakeup() method is a special method to interrupt consumer.poll()
			// it will throw the exception wakeUpExcption
			consumer.wakeup();
		}
		
	}
	
}
