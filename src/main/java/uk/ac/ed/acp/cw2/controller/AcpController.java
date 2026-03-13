package uk.ac.ed.acp.cw2.controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Connection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/v1/acp")
public class AcpController {
    private static final String ID = "s2814142";
    private final RuntimeEnvironment env;
    private final Gson gson = new Gson();

    public AcpController(RuntimeEnvironment env){
        this.env = env;
    }

    private Properties kafkaProps(){
        Properties p = new Properties();
        p.put("bootstrap.servers", env.getKafkaBootstrapServers());
        p.put("acks", "all");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("group.id", UUID.randomUUID().toString());
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "true");
        if(env.getKafkaSecurityProtocol()!=null){
            p.put("security.protocol", env.getKafkaSecurityProtocol());
            p.put("sasl.mechanism", env.getKafkaSaslMechanism());
            p.put("sasl.jaas.config", env.getKafkaSaslJaasConfig());
        }
        return p;
    }

    private Connection rabbitConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(env.getRabbitMqHost());
        factory.setPort(env.getRabbitMqPort());
        return factory.newConnection();
    }

    private List<String> readFromRabbit(String queueName, int count) throws Exception {
        List<String> messages = new ArrayList<>();
        try (Connection conn = rabbitConnection();
             Channel channel = conn.createChannel()){
            channel.queueDeclare(queueName, false, false, false, null);
            while(messages.size() < count) {
                GetResponse delivery = channel.basicGet(queueName, true);
                if(delivery != null){
                    messages.add(new String(delivery.getBody(), StandardCharsets.UTF_8));
                } else {
                    Thread.sleep(10);
                }
            }
        }
        return messages;
    }

    // PUT messages/rabbitmq/{queueName}/{messageCount}
    @PutMapping("/messages/rabbitmq/{queueName}/{messageCount}")
    public void putRabbitMessages(@PathVariable String queueName, @PathVariable int messageCount) throws Exception{
        try(Connection conn = rabbitConnection();
        Channel channel = conn.createChannel()) {
            channel.queueDeclare(queueName, false, false, false, null);
            for(int i = 0; i < messageCount; i++){
                JsonObject msg = new JsonObject();
                msg.addProperty("uid", ID);
                msg.addProperty("counter", i);
                channel.basicPublish("", queueName, null, gson.toJson(msg).getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e){
            System.out.print("ERROR: PUT messages/rabbitmq/{queueName}/{messageCount} - "+ e);
        }
    }

    // PUT messages/kafka/{writeTopic}/{messageCount}
    @PutMapping("/messages/kafka/{writeTopic}/{messageCount}")
    public void putKafkaMessages(@PathVariable String writeTopic, @PathVariable int messageCount) throws Exception{
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps())){
            for(int i = 0; i < messageCount; i++){
                JsonObject msg = new JsonObject();
                msg.addProperty("uid", ID);
                msg.addProperty("counter", i);
                producer.send(new ProducerRecord<>(writeTopic, String.valueOf(i), gson.toJson(msg))).get(1000, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e){
            System.out.print("ERROR: PUT messages/kafka/{writeTopic}/{messageCount} - "+ e);
        }
    }

    // GET messages/rabbitmq/{queueName}/{timeoutInMsec}
    @GetMapping("/messages/rabbitmq/{queueName}/{timeoutInMsec}")
    public List<String> getRabbitMessages (@PathVariable String queueName, @PathVariable int timeoutInMsec) throws Exception{
        List<String> result = new ArrayList<>();
        try (Connection conn = rabbitConnection();
        Channel channel = conn.createChannel()) {
            channel.queueDeclare(queueName, false, false, false, null);
            channel.basicConsume(queueName, true, (tag,delivery) -> result.add(new String(delivery.getBody(), StandardCharsets.UTF_8)), tag -> {});
            Thread.sleep(timeoutInMsec);
        }
        return result;
    }

    // GET messages/kafka/{readTopic}/{timeoutInMsec}
    @GetMapping("/messages/kafka/{readTopic}/{timeoutInMsec}")
    public List<String> getKafkaMessages(@PathVariable String readTopic, @PathVariable int timeoutInMsec){
        List<String> result = new ArrayList<>();
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps())) {
            consumer.subscribe(Collections.singletonList(readTopic));
            for(ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(timeoutInMsec))) {
                result.add(record.value());
            }
        }
        return result;
    }
}
