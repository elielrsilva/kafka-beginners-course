package com.github.elielodeveloper;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
	
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		
		String bootstrapServers = "127.0.0.1:9092";
		
		//Create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for(int i=0; i<10; i++) {
		
			//create a producer record
			
			String topic = "second_topic";
			String value = "hello world "+ Integer.toString(i);
			String key = "id_" + Integer.toString(i);
			
			
			ProducerRecord<String, String> record = 
					new ProducerRecord<String, String>(topic, key, value);
			
			logger.info("Key: " + key); // log he key
			
			// send data
			producer.send(record, new Callback () {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// execute every time a record is successfully sent or an exception is thrown
					if(exception == null){
						// the record was successfully sent
						logger.info("Received new metadata. \n" +
						"Topic: " + metadata.topic() + "\n" +
						"Partition: " + metadata.partition() + "\n" +
						"Offset: " + metadata.offset() + "\n" +
						"Timestamp" + metadata.timestamp());
					} else {
						logger.error("Error while producing", exception);
					}
				}
				
			}).get(); // block the .send() to make it synchonous - do not make it in production
			
		}
		
		
		
		//flush and close producer
		producer.flush();
		producer.close();
		
	}
}
