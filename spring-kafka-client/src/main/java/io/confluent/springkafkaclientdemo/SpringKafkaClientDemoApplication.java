package io.confluent.springkafkaclientdemo;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class SpringKafkaClientDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaClientDemoApplication.class, args);
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name("t1a_MK")
				.partitions(1)
				.replicas(1)
				.build();
	}

	@KafkaListener(id = "springClient", topics = "t1_MK")
	public void listen(String in) {
		System.out.println(in);
	}

}
