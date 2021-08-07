package com.juan.salinas.kafka.transactional;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalProducer {

	public static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); //Broker de Kafka 
		props.put("acks", "all");
		props.put("transactional.id", "devs4j-producer-id");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "10");
		try(Producer<String, String> producer = new  KafkaProducer<String, String>(props);){
			try {
				producer.initTransactions();
				producer.beginTransaction();
				for(int i = 0; i < 100000; i++) {
					producer.send(new ProducerRecord<String, String>("devs4j-topic", String.valueOf(i), "devs4j-value"));
					if(i == 50000) {
						throw new Exception("Unexpected Exception");
					}
				}
				producer.commitTransaction();
				producer.flush();
			}catch (Exception e) {
					log.error("Error ", e);
					producer.abortTransaction();
			}
		} 
		
	}
	
}
