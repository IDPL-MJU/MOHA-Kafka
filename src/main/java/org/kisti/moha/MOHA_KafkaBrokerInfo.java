package org.kisti.moha;

public class MOHA_KafkaBrokerInfo {
	private String kafkaLibsDir;
	private String appId;
	private String brokerId;
	private String containerId;	
	private String zookeeperConnect;
	private int port;

	private String hostname;
	private long launchedTime;
		
	public String getKafkaLibsDir() {
		return kafkaLibsDir;
	}

	public void setKafkaLibsDir(String kafkaLibVersion) {
		this.kafkaLibsDir = "kafkaLibs/" + kafkaLibVersion;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	

	public String getContainerId() {
		return containerId;
	}

	public void setContainerId(String containerId) {
		this.containerId = containerId;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public long getLaunchedTime() {
		return launchedTime;
	}

	public void setLaunchedTime(long launchedTime) {
		this.launchedTime = launchedTime;
	}

	public String getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(String brokerId) {
		this.brokerId = brokerId;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public void setZookeeperConnect(String zookeeperConnect) {
		this.zookeeperConnect = zookeeperConnect;
	}

}
