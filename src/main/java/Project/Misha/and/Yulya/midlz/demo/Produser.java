package Project.Misha.and.Yulya.midlz.demo;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Produser {

    private static final String TOPIC_NAME = "hello_topic";

    public static void main(String[] args) {

        // Настройки Kafka Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Создание Kafka Producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Отправка сообщения в Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Привет");
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println("Сообщение успешно отправлено в тему: " + metadata.topic());
                }
            }
        });

        producer.close();
    }
}
