package org.kisti.moha;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MOHA_Configuration {

	private String kafkaLibsDirs;
	private String kafkaVersion;
	private String kafkaClusterId;
	private String debugQueueName;
	private String enableMysqlLog;
	private String enableKafkaDebug;

	public MOHA_Configuration(String confDir) {
		Properties prop = new Properties();
		init();
		try {
			prop.load(new FileInputStream(confDir));		

			setDebugQueueName(prop.getProperty("MOHA.kafka.debug.queue.name","debug"));
			setKafkaClusterId(prop.getProperty("MOHA.kafka.cluster.id","Kafka/Cluster1"));
			setKafkaVersion(prop.getProperty("MOHA.dependencies.kafka.version","kafka_2.11-0.10.1.0"));
			setKafkaLibsDirs(prop.getProperty("MOHA.dependencies.kafka.libs","/usr/hdp/kafka_2.11-0.10.1.0/libs/*"));
			setEnableKafkaDebug(prop.getProperty("MOHA.kafka.debug.enable","false"));
			setEnableMysqlLog(prop.getProperty("MOHA.mysql.log.enable","false"));

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public MOHA_Configuration() {
		init();
	}

	private void init(){
		//initiate variables
		setDebugQueueName("debug");
		setKafkaClusterId("Kafka/Cluster1");
		setKafkaVersion("kafka_2.11-0.10.1.0");
		setKafkaLibsDirs("/usr/hdp/kafka_2.11-0.10.1.0/libs/*");
		setEnableKafkaDebug("false");
		setEnableMysqlLog("false");
	}




	@Override
	public String toString() {
		return "MOHA_Configuration [kafkaLibsDirs=" + kafkaLibsDirs + ", kafkaVersion=" + kafkaVersion
				+ ", kafkaClusterId=" + kafkaClusterId + ", debugQueueName=" + debugQueueName + ", enableMysqlLog="
				+ enableMysqlLog + ", enableKafkaDebug=" + enableKafkaDebug + "]";
	}


	//get configuration information
	List<String> getInfo(){
		List<String> conf = new ArrayList<>();
		conf.add(getKafkaLibsDirs());
		conf.add(getKafkaVersion());
		conf.add(getKafkaClusterId());
		conf.add(getDebugQueueName());
		conf.add(getEnableKafkaDebug());
		conf.add(getEnableMysqlLog());
		return conf;
	}

	public String getEnableMysqlLog() {
		return enableMysqlLog;
	}

	public void setEnableMysqlLog(String enableMysqlLog) {
		this.enableMysqlLog = enableMysqlLog;
	}

	public String getEnableKafkaDebug() {
		return enableKafkaDebug;
	}

	public void setEnableKafkaDebug(String enableKafkaDebug) {
		this.enableKafkaDebug = enableKafkaDebug;
	}

	public String getKafkaLibsDirs() {
		return kafkaLibsDirs;
	}

	public void setKafkaLibsDirs(String kafkaLibsDirs) {
		this.kafkaLibsDirs = kafkaLibsDirs;
	}

	public String getKafkaVersion() {
		return kafkaVersion;
	}

	public void setKafkaVersion(String kafkaVersion) {
		this.kafkaVersion = kafkaVersion;
	}

	public String getKafkaClusterId() {
		return kafkaClusterId;
	}

	public void setKafkaClusterId(String kafkaClusterId) {
		this.kafkaClusterId = kafkaClusterId;
	}

	public String getDebugQueueName() {
		return debugQueueName;
	}

	public void setDebugQueueName(String debugQueueName) {
		this.debugQueueName = debugQueueName;
	};
}
