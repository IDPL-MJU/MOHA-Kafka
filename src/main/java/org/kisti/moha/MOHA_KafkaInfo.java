package org.kisti.moha;

import org.mortbay.log.Log;

public class MOHA_KafkaInfo {
	private  String clusterName;
	private String kafkaClusterId;
	private  int brokerMem;
	private  int numBrokers;
	private  String libsPath;
	private  int numPartitions;
	private  long startingTime;
	private long makespan;
	private  int numCommands;
	private  String command;
	private long initTime;
	private long allocationTime;
	
	public long getAllocationTime() {
		return allocationTime;
	}
	public void setAllocationTime(long allocationTime) {
		this.allocationTime = allocationTime;
	}
	public long getInitTime() {
		return initTime;
	}
	public void setInitTime(long l) {
		this.initTime = l;
	}
	public String getKafkaClusterName() {
		return clusterName;
	}
	public void setKafkaClusterName(String clusterName) {
		Log.info("appId = {}", clusterName);
		this.clusterName = clusterName;
	}
	public int getBrokerMem() {
		return brokerMem;
	}
	public void setBrokerMem(int executorMemory) {
		this.brokerMem = executorMemory;
	}
	public int getNumBrokers() {
		return numBrokers;
	}
	public void setNumBrokers(int numExecutors) {
		this.numBrokers = numExecutors;
	}
	public String getLibsPath() {
		return libsPath;
	}
	public void setLibsPath(String jdlPath) {
		this.libsPath = jdlPath;
	}
	public int getNumPartitions() {
		return numPartitions;
	}
	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}
	public long getStartingTime() {
		return startingTime;
	}
	public void setStartingTime(long startingTime) {
		this.startingTime = startingTime;
	}
	public long getMakespan() {
		return makespan;
	}
	public void setMakespan(long makespan) {
		this.makespan = makespan;
	}
	public int getNumCommands() {
		return numCommands;
	}
	public void setNumCommands(int numCommands) {
		this.numCommands = numCommands;
	}
	public String getCommand() {
		return command;
	}
	public void setCommand(String command) {
		this.command = command;
	}
	public String getKafkaClusterId() {
		return kafkaClusterId;
	}
	public void setKafkaClusterId(String kafkaClusterId) {
		this.kafkaClusterId = kafkaClusterId;
	}
	
}
