package Project.Misha.and.Yulya.midlz.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;

public class Produser {

    private static final String TOPIC_NAME = "hello_topic";

    public static void main(String[] args) {

        // Настройки Kafka Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "how_are_you_group");

        // Создание Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Подписка на тему
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        // Чтение сообщений из Kafka
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Получено сообщение: " + record.value());
                System.out.println("Отправка ответа: Как дела?");
            }

            // Подтверждение обработки сообщений
            consumer.commitAsync();
        }
    }
}