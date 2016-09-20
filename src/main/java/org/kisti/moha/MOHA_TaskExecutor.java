package org.kisti.moha;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MOHA_TaskExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_TaskExecutor.class);
	private String hostname;
	private YarnConfiguration conf;

	private String appId;
	private final String inputQueueName;
	private final String outputQueueName;
	private final String containerId;
	private final String id;
	private static MOHA_Queue inQueue;
	private static MOHA_Queue outQueue;
	
	private static MOHA_Queue mohaStatistic;
	private MOHA_ExecutorInfo info;
	private MOHA_Database data;

	public MOHA_TaskExecutor(String[] args) throws IOException {
		// TODO Auto-generated constructor stub
		info = new MOHA_ExecutorInfo();
		info.setLaunchedTime(System.currentTimeMillis());
			
		hostname = NetUtils.getHostname();
		conf = new YarnConfiguration();

		FileSystem.get(conf);

		LOG.info("Start using kafka");

		appId = args[0];
		inputQueueName = appId + MOHA_Properties.inputQueue;
		// outputQueue = queueName + MOHA_Properties.outputQueue;
		outputQueueName = "test";// just for testing
		containerId = args[1];
		id = args[2];

		mohaStatistic = new MOHA_Queue(MOHA_Properties.mohaStatistic);
		mohaStatistic.register();
		
		outQueue = new MOHA_Queue(outputQueueName);
		outQueue.register();
		LOG.info(outQueue.push("Start MOHA_TaskExecutor constructor on " + id));

		inQueue = new MOHA_Queue(inputQueueName);
		inQueue.subcribe();	
		
		
		info.setAppId(appId);
		info.setExecutorId(id);		
		info.setContainerId(containerId);
		info.setHostname(this.hostname);
		
		
		data = new MOHA_Database();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LOG.info("Container just started on {}" + NetUtils.getHostname());
		
		try {
			MOHA_TaskExecutor executor = new MOHA_TaskExecutor(args);
			executor.run();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.info(outQueue.push("Executor is ending ..."));
	}

	private void run() {
		// TODO Auto-generated method stub
		LOG.info(outQueue.push("Executor is running on " + this.hostname));
		LOG.info(outQueue.push("Executor is running on " + id));
		LOG.info(outQueue.push("Queue Name : " + inputQueueName));
		long startingTime = System.currentTimeMillis();
		long expiredTime = System.currentTimeMillis() + 10000;
		int numComand = 0;
		int pollingTime = 0;
		int retries = 2;
		boolean found = false; 
		info.setEndingTime(startingTime);
		while (System.currentTimeMillis() < expiredTime) {
			long fistMes = System.currentTimeMillis();
			ConsumerRecords<Integer, String> records = inQueue.poll(1000);
			if(records.count() > 0){
				info.setFirstMessageTime(fistMes);
				found = true;			
			}

/*			for (ConsumerRecord<Integer, String> record : records) {

				List<String> command = new ArrayList<String>();
				
				String[] str = record.value().split(" ");
				for(String cmd: str){
					command.add(cmd);
				}
				ProcessBuilder builder = new ProcessBuilder(command);
				Process p;
				//String line;
				try {
					p = builder.start();
					p.waitFor();
					BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
					while ((line = br.readLine()) != null) {
						outQueue.push(
								"Task Executor (" + id + ") " + "from partition  " + record.partition() + " : " + line);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//numComand++;			
				//info.setFirstMessageTime(System.currentTimeMillis());
			}*/
			/*for (ConsumerRecord<Integer, String> record : records) {
				//LOG.info("command = {}",record.value());
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			if(retries < 3){
				//outQueue.push("count = "+records.count());
			}
			//LOG.info("count = {}",records.count());
			if ((records.count() > 0)&&((expiredTime - startingTime)<480000)){
				//LOG.info("Before commit");
				inQueue.commitSync();
				//LOG.info("After commit");
				
				pollingTime ++;
				numComand += records.count();		
				info.setEndingTime(System.currentTimeMillis());
				/*try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				expiredTime = System.currentTimeMillis() + 2000;
			}else if ((retries>0)&&found){
				LOG.info(outQueue.push("Executor " + id + " : Re-poll messages"));
				//inQueue.subcribe();
				/*try {
					Thread.sleep(5000);
					inQueue.commitSync();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				retries --;
				found = false;
				expiredTime = System.currentTimeMillis() + 2000;*/
			}

		}
		LOG.info(outQueue.push("TaskExecutor (" + id +")----------- There are  " + numComand + " (commands) have been executed"+ "  SleepTime: " + pollingTime));
		long executingTime = info.getEndingTime() - info.getFirstMessageTime();
		info.setRunningTime(executingTime);
		info.setNumExecutedTasks(numComand);
		info.setPollingTime(pollingTime);		
		info.setEndingTime(System.currentTimeMillis());
		data.executorInsert(info);
		mohaStatistic.push(id + " " + numComand + " " + executingTime);
		inQueue.close();
	}

}
