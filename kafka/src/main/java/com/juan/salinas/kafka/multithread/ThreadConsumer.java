package com.juan.salinas.kafka.multithread;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadConsumer extends Thread{

	public static final Logger log = LoggerFactory.getLogger(ThreadConsumer.class);
	
	private final AtomicBoolean closed = new  AtomicBoolean(false);
	private final KafkaConsumer<String, String> consumer;
	
	public ThreadConsumer(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}
	
	
	@Override
	public void run() {
		consumer.subscribe(Arrays.asList("devs4j-topic"));
		try {
			while(!closed.get()) {
					ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
						log.debug("Offset = {} , Partition = {} , Key = {} , Value = {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
						if(Integer.parseInt(consumerRecord.key()) % 100000 == 0) {
							log.info("Offset = {} , Partition = {} , Key = {} , Value = {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
						}
					}
			}
		}catch (WakeupException e) {
			if(!closed.get()) {
				throw e;
			}
		}finally {
			consumer.close();
		}
	}

	
	public void shutDown() {
		closed.set(true);
		consumer.wakeup();
	}
	
}
