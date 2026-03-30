package uk.ac.ed.acp.cw2.controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Connection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
    private static final Logger log = LoggerFactory.getLogger(AcpController.class);

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
            channel.queueDeclare(queueName, true, false, false, null);
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
        log.info("PUT rabbitmq -> queue={} count={}", queueName, messageCount);
        try(Connection conn = rabbitConnection();
        Channel channel = conn.createChannel()) {
            channel.queueDeclare(queueName, true, false, false, null);
            for(int i = 0; i < messageCount; i++){
                JsonObject msg = new JsonObject();
                msg.addProperty("uid", ID);
                msg.addProperty("counter", i);
                channel.basicPublish("", queueName, null, gson.toJson(msg).getBytes(StandardCharsets.UTF_8));
            }
            log.info("PUT rabbitmq -> wrote {} messages to {}", messageCount, queueName);
        } catch (Exception e){
            log.error("PUT rabbitmq error", e);
        }
    }

    // PUT messages/kafka/{writeTopic}/{messageCount}
    @PutMapping("/messages/kafka/{writeTopic}/{messageCount}")
    public void putKafkaMessages(@PathVariable String writeTopic, @PathVariable int messageCount) throws Exception{
        log.info("PUT kafka -> topic={} count={}", writeTopic, messageCount);
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps())){
            for(int i = 0; i < messageCount; i++){
                JsonObject msg = new JsonObject();
                msg.addProperty("uid", ID);
                msg.addProperty("counter", i);
                producer.send(new ProducerRecord<>(writeTopic, String.valueOf(i), gson.toJson(msg))).get(1000, TimeUnit.MILLISECONDS);
            }
            log.info("PUT kafka -> wrote {} messages to {}", messageCount, writeTopic);
        } catch (Exception e){
            log.error("PUT kafka error", e);
        }
    }

    // GET messages/rabbitmq/{queueName}/{timeoutInMsec}
    @GetMapping("/messages/rabbitmq/{queueName}/{timeoutInMsec}")
    public List<String> getRabbitMessages (@PathVariable String queueName, @PathVariable int timeoutInMsec) throws Exception{
        log.info("GET rabbitmq -> queue={} timeout={}ms", queueName, timeoutInMsec);
        List<String> result = new ArrayList<>();
        try (Connection conn = rabbitConnection();
        Channel channel = conn.createChannel()) {
            channel.queueDeclare(queueName, true, false, false, null);
            channel.basicConsume(queueName, true, (tag,delivery) -> result.add(new String(delivery.getBody(), StandardCharsets.UTF_8)), tag -> {});
            Thread.sleep(timeoutInMsec);
        } catch (Exception e){
            log.error("GET rabbitmq error", e);
        }
        log.info("GET rabbitmq -> returning {} messages", result.size());
        return result;
    }

    // GET messages/kafka/{readTopic}/{timeoutInMsec}
    @GetMapping("/messages/kafka/{readTopic}/{timeoutInMsec}")
    public List<String> getKafkaMessages(@PathVariable String readTopic, @PathVariable int timeoutInMsec) throws Exception{
        log.info("GET kafka -> topic={} timeout={}ms", readTopic, timeoutInMsec);
        List<String> result = new ArrayList<>();
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps())) {
            consumer.subscribe(Collections.singletonList(readTopic));
            for(ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(timeoutInMsec))) {
                result.add(record.value());
            }
        } catch (Exception e){
            log.error("GET kafka error", e);
        }
        log.info("GET kafka -> returning {} messages", result.size());
        return result;
    }

    // GET messages/sorted/rabbitmq/{queueName}/{messagesToConsider}
    @GetMapping("/messages/sorted/rabbitmq/{queueName}/{messagesToConsider}")
    public List<String> getSortedRabbitMessages(@PathVariable String queueName, @PathVariable int messagesToConsider) throws Exception {
        log.info("GET sorted rabbitmq -> queue={} n={}", queueName, messagesToConsider);
        List<String> messages = readFromRabbit(queueName, messagesToConsider);
        messages.sort((a,b) -> {
            int idA = JsonParser.parseString(a).getAsJsonObject().get("Id").getAsInt();
            int idB = JsonParser.parseString(b).getAsJsonObject().get("Id").getAsInt();
            return Integer.compare(idA, idB);
        });
        log.info("GET sorted rabbitmq -> returning {} sorted messages", messages.size());
        return messages;
    }

    // GET messages/sorted/kafka/{topic}/{messagesToConsider}
    @GetMapping("/messages/sorted/kafka/{topic}/{messagesToConsider}")
    public List<String>  getSortedKafkaMessages(@PathVariable String topic, @PathVariable int messagesToConsider) throws Exception {
        log.info("GET sorted kafka -> topic={} n={}", topic, messagesToConsider);
        List<String> messages = new ArrayList<>();
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps())) {
            consumer.subscribe(Collections.singletonList(topic));
            while(messages.size() < messagesToConsider) {
                for(ConsumerRecord<String, String> record: consumer.poll(Duration.ofMillis(500))) {
                    messages.add(record.value());
                    if (messages.size() >= messagesToConsider){
                        break;
                    }
                }
            }
            messages.sort((a,b) -> {
                int idA = JsonParser.parseString(a).getAsJsonObject().get("Id").getAsInt();
                int idB = JsonParser.parseString(b).getAsJsonObject().get("Id").getAsInt();
                return Integer.compare(idA, idB);
            });

        } catch (Exception e){
            log.error("GET sorted kafka error", e);
        }
        log.info("GET sorted kafka -> returning {} sorted messages", messages.size());
        return messages;
    }

    // POST splitter
    @PostMapping("/splitter")
    public void splitter(@RequestBody Map<String, Object> body) throws Exception {
        String readQueue = (String) body.get("readQueue");
        String writeTopicOdd = (String) body.get("writeTopicOdd");
        String redisHashOdd = (String) body.get("redisHashOdd");
        String writeTopicEven = (String) body.get("writeTopicEven");
        String redisHashEven = (String) body.get("redisHashEven");
        int messageCount = ((Number) body.get("messageCount")).intValue();
        log.info("POST splitter -> readQueue={} count={}", readQueue, messageCount);
        List<String> messages = readFromRabbit(readQueue, messageCount);
        try (JedisPool pool = new JedisPool(env.getRedisHost(), env.getRedisPort());
             Jedis jedis = pool.getResource();
             KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps())
        ) {
            long countEven = jedis.exists("count_even") ? Long.parseLong(jedis.get("count_even")) : 0;
            long countOdd = jedis. exists("count_odd") ? Long.parseLong(jedis.get("count_odd")) : 0;
            double sumEven = jedis.exists("average_even") ? Double.parseDouble(jedis.get("average_even")) * countEven : 0.0;
            double sumOdd = jedis.exists("average_odd") ? Double.parseDouble(jedis.get("average_odd")) * countOdd : 0.0;
            for(String msg : messages){
                JsonObject obj = JsonParser.parseString(msg).getAsJsonObject();
                int id = obj.get("Id").getAsInt();
                double value = obj.get("Value").getAsDouble();
                if(id%2==0) {
                    jedis.hset(redisHashEven, String.valueOf(id), msg);
                    countEven++;
                    sumEven += value;
                    jedis.set("count_even", String.valueOf(countEven));
                    jedis.set("average_even", String.valueOf(round(sumEven/countEven)));
                    producer.send(new ProducerRecord<>(writeTopicEven, String.valueOf(id),msg)).get(1000, TimeUnit.MILLISECONDS);
                    log.info("  EVEN id={} value={} -> topic={}", id, value, writeTopicEven);
                } else {
                    jedis.hset(redisHashOdd, String.valueOf(id), msg);
                    countOdd++;
                    sumOdd += value;
                    jedis.set("count_odd", String.valueOf(countOdd));
                    jedis.set("average_odd", String.valueOf(round(sumOdd/countOdd)));
                    producer.send(new ProducerRecord<>(writeTopicOdd, String.valueOf(id),msg)).get(1000, TimeUnit.MILLISECONDS);
                    log.info("  ODD  id={} value={} -> topic={}", id, value, writeTopicOdd);
                }
            }
            log.info("POST splitter done -> countEven={} countOdd={}", countEven, countOdd);
        } catch (Exception e){
            log.error("POST splitter error", e);
        }
    }

    // POST transformMessages
    @PostMapping("/transformMessages")
    public void transformMessages(@RequestBody Map<String, Object> body) throws Exception {
        String readQueue = (String) body.get("readQueue");
        String writeQueue = (String) body.get("writeQueue");
        int messageCount = ((Number) body.get("messageCount")).intValue();
        log.info("POST transformMessages -> readQueue={} writeQueue={} count={}", readQueue, writeQueue, messageCount);
        int totalAdded10 = 0;
        int totalWritten = 0;
        int tombstoneCount = 0;
        double totalValue = 0.0;
        double totalAdded = 0.0;

        try (
                Connection connection = rabbitConnection();
                Channel readChannel = connection.createChannel();
                Channel writeChannel = connection.createChannel();
                JedisPool pool = new JedisPool(env.getRedisHost(), env.getRedisPort());
                Jedis jedis = pool.getResource()
                ) {
            readChannel.queueDeclare(readQueue, true, false, false, null);
            writeChannel.queueDeclare(writeQueue, true, false, false, null);
            Set<String> redisKeys = new HashSet<>();
            for(int i = 0; i < messageCount; i++){
                GetResponse delivery = null;
                while(delivery==null){
                    delivery = readChannel.basicGet(readQueue, true);
                    if(delivery==null){
                        Thread.sleep(10);
                    }
                }
                String raw = new String(delivery.getBody(), StandardCharsets.UTF_8);
                JsonObject obj = JsonParser.parseString(raw).getAsJsonObject();
                String key = obj.get("key").getAsString();
                if("TOMBSTONE".equals(key)){
                    tombstoneCount++;
                    log.info("TOMBSTONE hit, tombstoneCount={}", tombstoneCount);
                    for (String k : redisKeys) {
                        jedis.del(k);
                    }
                    redisKeys.clear();
                    totalWritten++;
                    JsonObject stats = new JsonObject();
                    stats.addProperty("totalMessagesWritten", totalWritten);
                    stats.addProperty("totalMessagesProcessed", totalAdded10);
                    stats.addProperty("totalRedisUpdates", totalAdded10 + tombstoneCount);
                    stats.addProperty("totalValueWritten", totalValue);
                    stats.addProperty("totalAdded", totalAdded);
                    writeChannel.basicPublish("", writeQueue, null, gson.toJson(stats).getBytes(StandardCharsets.UTF_8));
                    log.info("TOMBSTONE stats written: {}", gson.toJson(stats));
                } else {
                    int version = obj.get("version").getAsInt();
                    double value = obj.get("value").getAsDouble();
                    String storedString = jedis.get(key);
                    int storedVersion = storedString != null ? Integer.parseInt(storedString) : -1;
                    String outMsg;
                    if(storedVersion < version){
                        jedis.set(key, String.valueOf(version));
                        redisKeys.add(key);
                        totalAdded10++;
                        JsonObject out = new JsonObject();
                        out.addProperty("key", key);
                        out.addProperty("version", version);
                        out.addProperty("value", value + 10.5);
                        outMsg = gson.toJson(out);
                        totalAdded += 10.5;
                        totalValue += (value + 10.5);
                        log.info("NEW/UPDATE key={} v={} value={}->{}",key, version, value, value+10.5);
                    } else {
                        outMsg = raw;
                        totalValue += value;
                        log.info("PASSTHROUGH key={} v={} (stored={})", key, version, storedVersion);
                    }
                    writeChannel.basicPublish("", writeQueue, null, outMsg.getBytes(StandardCharsets.UTF_8));
                    totalWritten++;
                }
            }
            log.info("POST transformMessages done -> processed={} written={} redisUpdates={}", totalAdded10, totalWritten, tombstoneCount);
        } catch (Exception e){
            log.error("POST transformMessages error", e);
        }
    }

    private double round(double value){
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }
}
