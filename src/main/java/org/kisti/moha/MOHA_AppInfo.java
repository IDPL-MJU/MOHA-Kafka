package org.kisti.moha;

import org.mortbay.log.Log;

public class MOHA_AppInfo {
	private  String appId;
	private  int executorMemory;
	private  int numExecutors;
	private  String jdlPath;
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
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		Log.info("appId = {}", appId);
		this.appId = appId;
	}
	public int getExecutorMemory() {
		return executorMemory;
	}
	public void setExecutorMemory(int executorMemory) {
		this.executorMemory = executorMemory;
	}
	public int getNumExecutors() {
		return numExecutors;
	}
	public void setNumExecutors(int numExecutors) {
		this.numExecutors = numExecutors;
	}
	public String getJdlPath() {
		return jdlPath;
	}
	public void setJdlPath(String jdlPath) {
		this.jdlPath = jdlPath;
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
	
}
