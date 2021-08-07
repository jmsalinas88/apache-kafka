package com.juan.salinas.kafka.producers;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyProducer {

	public static final Logger log = LoggerFactory.getLogger(MyProducer.class);
	
	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); //Broker de Kafka 
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "10");
		
		try(Producer<String, String> producer = new  KafkaProducer<String, String>(props);){
			for(int i = 0; i < 100000; i++) {
				producer.send(new ProducerRecord<String, String>("devs4j-topic", String.valueOf(i), "devs4j-value"));
			}
			producer.flush();
		} 
//		catch (InterruptedException | ExecutionException e) {
//			log.error("Message producer interrupd", e);
//		}

	}

}
