package com.juan.salinas.apachekafka;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;

import io.micrometer.core.instrument.MeterRegistry;

@SpringBootApplication
public class ApachekafkaApplication {

	private static final Logger log = LoggerFactory.getLogger(ApachekafkaApplication.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	@Autowired
	private KafkaListenerEndpointRegistry registry;
	@Autowired
	private MeterRegistry meterRegistry;

	@KafkaListener(id = "devs4jId", autoStartup = "true", topics = "devs4j-topic", containerFactory = "listenerContainerFactory", groupId = "devs4j-group", properties = {
			"max.poll.interval.ms:4000", "max.poll.records:50" })
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Messages received = {}", messages.size());
		//log.info("Start reading messages");
		for (ConsumerRecord<String, String> message : messages) {
			// log.info("Partition = {} , Offset= {}, Key = {} , Value {} ",
			// message.partition(), message.offset(), message.key(), message.value());
		}
		//log.info("Batch complete");
	}

	public static void main(String[] args) {
		SpringApplication.run(ApachekafkaApplication.class, args);
	}

	/*
	 * @Override public void run(String... args) throws Exception { for(int i = 0; i
	 * < 100; i++) { //kafkaTemplate.send("devs4j-topic",String.valueOf(i),String.
	 * format("Sample message %d", i)); } //Sincrono:
	 * //kafkaTemplate.send("devs4j-topic","Sample message ").get(100,
	 * TimeUnit.MILLISECONDS);
	 * 
	 * // Asincrono + Callback
	 * 
	 * ListenableFuture<SendResult<String, String>> future =
	 * kafkaTemplate.send("devs4j-topic","Sample message "); future.addCallback(new
	 * KafkaSendCallback<String, String>() {
	 * 
	 * @Override public void onSuccess(SendResult<String, String> result) {
	 * log.info("Message sent ", result.getRecordMetadata().offset()); }
	 * 
	 * @Override public void onFailure(Throwable ex) {
	 * log.error("Error sending message ", ex); }
	 * 
	 * @Override public void onFailure(KafkaProducerException ex) {
	 * log.error("Error sending message ", ex); } });
	 * 
	 * }
	 */

	@Scheduled(fixedDelay = 2000, initialDelay = 100)
	public void sendKafkaMessages() {
		for (int i = 0; i < 200; i++) {
			kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample message %d", i));
		}
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void printMetrics() {
		double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
		log.info("Count {} ", count);
	}

}
