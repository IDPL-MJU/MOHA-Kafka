package org.kisti.moha;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MOHA_TaskExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_TaskExecutor.class);
	
	private YarnConfiguration conf;


	private static MOHA_Queue inQueue;
	private static MOHA_Logger debugLogger;
	
	private MOHA_ExecutorInfo info;
	private MOHA_Database data;

	public MOHA_TaskExecutor(String[] args) throws IOException {
		// TODO Auto-generated constructor stub
		info = new MOHA_ExecutorInfo();
		info.setAppId(args[0]);
		info.setContainerId(args[1]);
		info.setExecutorId(args[2]);			
		info.setHostname(NetUtils.getHostname());
		info.setLaunchedTime(System.currentTimeMillis());
		info.setQueueName(info.getAppId());
			
		
		conf = new YarnConfiguration();

		FileSystem.get(conf);

		LOG.info("Start using kafka");

		

		debugLogger = new MOHA_Logger();
		
		LOG.info(debugLogger.info("Start MOHA_TaskExecutor constructor on " + info.getAppId()));

		inQueue = new MOHA_Queue(info.getQueueName());
		inQueue.subcribe();	
		
		
		
		
		
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
		LOG.info(debugLogger.info("Executor is ending ..."));
	}

	private void run() {
		// TODO Auto-generated method stub
		LOG.info(debugLogger.info("Executor is running on " + info.getHostname()));
		LOG.info(debugLogger.info("Executor is running on " + info.getAppId()));
		LOG.info(debugLogger.info("Queue Name : " + info.getQueueName()));
		
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
			if ((records.count() > 0)&&((expiredTime - startingTime)<48000)){
				//LOG.info("Before commit");
				LOG.info(debugLogger.info("inQueue.commitSync()"));
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
				LOG.info(debugLogger.info("Executor " + info.getAppId() + " : Re-poll messages"));
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
		LOG.info(debugLogger.info("TaskExecutor (" + info.getAppId() +")----------- There are  " + numComand + " (commands) have been executed"+ "  SleepTime: " + pollingTime));
		long executingTime = info.getEndingTime() - info.getFirstMessageTime();
		info.setRunningTime(executingTime);
		info.setNumExecutedTasks(numComand);
		info.setPollingTime(pollingTime);		
		info.setEndingTime(System.currentTimeMillis());
		data.executorInsert(info);
		
		inQueue.close();
	}

}
