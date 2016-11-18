package org.kisti.moha;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;

import kafka.utils.ZkUtils;
import kafka.utils.ZkUtils$;

public class MOHA_Queue {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_Queue.class);

	private final int sessionTimeout = 30000;
	private final int connectionTimeout = 30000;
	private ZkClient zkClient;
	private ZkConnection zkConnection;

	private ZkUtils zkUtils;
	private KafkaConsumer<String, String> consumer;
	private Producer<String, String> producer;
	private String queueName;
	private String clusterId = "";
	private String bootstrapServers = "localhost:9092";
	private String zookeeperConnect = "localhost:2181";
	MOHA_Zookeeper zk;

	public MOHA_Queue(String queueName) {
		this.queueName = queueName;
	}

	public MOHA_Queue(String clusterId, String queueName) {
		this.queueName = queueName;
		this.clusterId = clusterId;
		init();
	}

	private void init() {
		zk = new MOHA_Zookeeper(clusterId);

		try {
			bootstrapServers = zk.getBootstrapServers();
		} catch (IOException | InterruptedException | KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		zookeeperConnect = "localhost:2181/" + clusterId;
	}

	// Create queue
	public boolean create(int numPartitions, int numReplicationFactor) {

		zkClient = ZkUtils$.MODULE$.createZkClient(zookeeperConnect, sessionTimeout, connectionTimeout);
		zkConnection = new ZkConnection(zookeeperConnect, sessionTimeout);
		zkUtils = new ZkUtils(zkClient, zkConnection, false);
		AdminUtils.createTopic(zkUtils, queueName, numPartitions, numReplicationFactor, new Properties(), null);
		return true;

	}

	// Delete queue
	public boolean deleteQueue() {
		AdminUtils.deleteTopic(zkUtils, queueName);
		return true;
	}

	// Register to push messages to queue
	public boolean register() {
		LOG.info("bootstramservers = {}", bootstrapServers);
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);

		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
		return true;
	}

	public boolean unregister() {
		producer.close();
		zk.close();
		return true;
	}

	public boolean commitSync() {
		consumer.commitSync();
		return true;
	}

	// Subscribe to poll messages from queue
	public boolean subcribe() {
		Properties props = new Properties();
		// int fetch_size = 64;

		// props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// props.put(ConsumerConfig.GROUP_ID_CONFIG, queueName);
		// props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		// props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		// props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		// props.put("request.timeout.ms", "40000");
		//
		// //props.put("max.partition.fetch.bytes", String.valueOf(fetch_size));
		// props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
		// "org.apache.kafka.common.serialization.IntegerDeserializer");
		// props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
		// "org.apache.kafka.common.serialization.StringDeserializer");

		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id",queueName);
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "10000");
		props.put("request.timeout.ms", "40000");
		props.put("max.partition.fetch.bytes", String.valueOf(1024));
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<>(props);

		consumer.subscribe(Arrays.asList(queueName));

		return true;
	}

	public boolean close() {
		consumer.close();
		zk.close();
		return true;
	}

	public String push(String key, String messages) {

		producer.send(new ProducerRecord<String, String>(queueName, key, messages));
		return messages;
	}

	public String push(String messages) {

		producer.send(new ProducerRecord<String, String>(queueName, "0", messages));
		return messages;
	}

	public ConsumerRecords<String, String> poll(int timeOut) {
		ConsumerRecords<String, String> records = consumer.poll(timeOut);
		return records;
	}
}
