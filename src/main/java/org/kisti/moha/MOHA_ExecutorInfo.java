package org.kisti.moha;

public class MOHA_ExecutorInfo {
	private String appId;
	private String executorId;
	private String containerId;
	private long firstMessageTime;
	
	private String hostname;
	private long launchedTime;
	private int numExecutedTasks;
	private long runningTime;
	private int pollingTime;
	private long endingTime;
	public long getEndingTime() {
		return endingTime;
	}
	public void setEndingTime(long endingTime) {
		this.endingTime = endingTime;
	}
	public long getFirstMessageTime() {
		return firstMessageTime;
	}
	public void setFirstMessageTime(long firstMessageTime) {
		if(this.firstMessageTime == 0){
			this.firstMessageTime = firstMessageTime;
		}
		
	}
	public int getPollingTime() {
		return pollingTime;
	}
	public void setPollingTime(int pollingTime) {
		this.pollingTime = pollingTime;
	}
	public String getContainerId() {
		return containerId;
	}
	public void setContainerId(String containerId) {
		this.containerId = containerId;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public String getExecutorId() {
		return executorId;
	}
	public void setExecutorId(String executorId) {
		this.executorId = executorId;
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
	public int getNumExecutedTasks() {
		return numExecutedTasks;
	}
	public void setNumExecutedTasks(int numExecutedTasks) {
		this.numExecutedTasks = numExecutedTasks;
	}
	public long getRunningTime() {
		return runningTime;
	}
	public void setRunningTime(long exitTime) {
		this.runningTime = exitTime;
	}
}
