package org.kisti.moha;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MOHA_TaskExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_TaskExecutor.class);
	private static final int SESSION_MAXTIME = 30 * 60 * 1000;
	private static final int EXTENDED_SESSION_TIME = 2 * 1000;

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

		debugLogger = new MOHA_Logger(Boolean.parseBoolean(info.getConf().getKafkaDebugEnable()),
				info.getConf().getDebugQueueName());

		LOG.info(debugLogger.info("Start MOHA_TaskExecutor constructor on " + info.getAppId()));

		String zookeeperDir = info.getConf().getKafkaClusterId();
		MOHA_Zookeeper zk = new MOHA_Zookeeper(zookeeperDir);
		String zookeeperConnect = System.getenv(MOHA_Properties.ZOOKEEPER_CONNECT);
		String bootstrapServer = "localhost:9092";
		try {
			bootstrapServer = zk.getBootstrapServers();
		} catch (IOException | InterruptedException | KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		inQueue = new MOHA_Queue(zookeeperConnect, bootstrapServer, info.getQueueName());
		inQueue.subcribe();

		data = new MOHA_Database(Boolean.parseBoolean(info.getConf().getMysqlLogEnable()));
		LOG.info("Environment variable = {}", info.getConf().toString());
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

	}

	private void run() {
		// TODO Auto-generated method stub
		LOG.info(debugLogger.info("Executor is running on " + info.getHostname()));
		LOG.info(debugLogger.info("Executor is running on " + info.getAppId()));
		LOG.info(debugLogger.info("Queue Name : " + info.getQueueName()));

		long startingTime = System.currentTimeMillis();
		long expiredTime = System.currentTimeMillis() + 5 * EXTENDED_SESSION_TIME;
		int numComand = 0;
		int pollingTime = 0;
		int retries = 3;
		boolean found = false; // get first message from the queue
		info.setEndingTime(startingTime);

		while (System.currentTimeMillis() < expiredTime) {

			ConsumerRecords<String, String> records = inQueue.poll(1000);
			if (records.count() > 0) {
				info.setFirstMessageTime(System.currentTimeMillis());
				found = true;
			}

			for (ConsumerRecord<String, String> record : records) {
				debugLogger.info("TaskExecutor [" + info.getExecutorId() + "]: " + record.toString());

				List<String> command = new ArrayList<String>();

				String[] str = record.value().split(" ");
				for (String cmd : str) {
					command.add(cmd);
				}
				ProcessBuilder builder = new ProcessBuilder(command);
				Process p;
				String line;
				try {
					p = builder.start();
					p.waitFor();
					BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
					while ((line = br.readLine()) != null) {
						debugLogger.info("Task Executor (" + info.getExecutorId() + ") " + "from partition  "
								+ record.partition() + " : " + line);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

			if ((records.count() > 0) && ((expiredTime - startingTime) < SESSION_MAXTIME)) {

				LOG.info(debugLogger.info("inQueue.commitSync()"));
				inQueue.commitSync();
				pollingTime++;
				numComand += records.count();
				info.setEndingTime(System.currentTimeMillis());
				expiredTime = System.currentTimeMillis() + EXTENDED_SESSION_TIME;
			} else if ((retries > 0) && found) {
				LOG.info(debugLogger.info("TaskExecutor[" + info.getExecutorId() + "]: Re-poll messages"));
				// inQueue.subcribe();
				try {
					Thread.sleep(5000);
					inQueue.commitSync();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				retries--;
				expiredTime = System.currentTimeMillis() + EXTENDED_SESSION_TIME;
			}

		}
		LOG.info(debugLogger.info("TaskExecutor[" + info.getExecutorId() + "] : There are  " + numComand
				+ " (commands) have been executed" + "  SleepTime: " + pollingTime));
		long executingTime = info.getEndingTime() - info.getFirstMessageTime();
		info.setRunningTime(executingTime);
		info.setNumExecutedTasks(numComand);
		info.setPollingTime(pollingTime);
		info.setEndingTime(System.currentTimeMillis());
		data.insertExecutorInfoToDatabase(info);

		inQueue.close();
		LOG.info(debugLogger.info("TaskExecutor[" + info.getExecutorId() + "] is ending ..."));
	}

}
