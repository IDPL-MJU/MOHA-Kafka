package org.kisti.moha;

public interface MOHA_Properties {

	public static final String JDL = "moha.jdl";
	public static final String DB = "jdbc:mysql://150.183.250.139/moha";
	public static final String DB_EXECUTOR_TABLE_NAME = "exe";
	public static final String DB_APP_TABLE_NAME = "app";
	public static final String KAFKA_LOG_DIR = "kafka-log";
	public static final String KAFKA_SERVER_PROPERTIES = "broker.properties";
	public static final String KAKFA_VERSION = "KAKFA_VERSION";
	public static final String KAFKA_CLUSTER_ID = "KAFKA_CLUSTER_ID";
	public static final String KAFKA_DEBUG_QUEUE_NAME = "KAFKA_DEBUG_QUEUE_NAME";
	public static final String KAFKA_DEBUG_ENABLE = "KAFKA_DEBUG_ENABLE";
	public static final String MYSQL_DEBUG_ENABLE = "MYSQL_DEBUG_ENABLE";
	public static final String ZOOKEEPER_BOOTSTRAP_SERVER = "ZOOKEEPER_BOOTSTRAP_SERVER";
	public static final String ZOOKEEPER_CONNECT = "ZOOKEEPER_CONNECT";
	public static final String APP_JAR = "APP_JAR";
	public static final String APP_JAR_TIMESTAMP = "APP_JAR_TIMESTAMP";
	public static final String APP_JAR_SIZE = "APP_JAR_SIZE";
	public static final String KAFKA_TGZ = "KAFKA_TGZ";
	public static final String KAFKA_TGZ_TIMESTAMP = "KAFKA_TGZ_TIMESTAMP";
	public static final String KAFKA_TGZ_SIZE = "KAFKA_TGZ_SIZE";
}