package org.kisti.moha;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.SequentialNumber;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class MOHA_TaskExecutor {

	private YarnConfiguration conf;
	private static MOHA_Queue jobQueue;
	private static MOHA_Logger LOG;
	private MOHA_ExecutorInfo info;
	private MOHA_Database database;
	private long tempDockingTime;

	public MOHA_TaskExecutor(String[] args) throws IOException {
		// TODO Auto-generated constructor stub
		/* Save input parameters */
		info = new MOHA_ExecutorInfo();
		info.setAppId(args[0]);
		info.setContainerId(args[1]);
		info.setExecutorId(Integer.parseInt(args[2]));
		info.setHostname(NetUtils.getHostname());
		info.setLaunchedTime(0);
		info.setQueueName(info.getAppId());

		String zookeeperConnect = System.getenv(MOHA_Properties.KAFKA_ZOOKEEPER_CONNECT);
		String bootstrapServer = System.getenv(MOHA_Properties.KAFKA_ZOOKEEPER_BOOTSTRAP_SERVER);
		String queueType = System.getenv(MOHA_Properties.CONF_QUEUE_TYPE);

		LOG = new MOHA_Logger(MOHA_TaskExecutor.class, Boolean.parseBoolean(info.getConf().getKafkaDebugEnable()), info.getConf().getDebugQueueName(), zookeeperConnect, bootstrapServer,
				info.getAppId(), info.getExecutorId());
		LOG.register();

		LOG.all("TaskExecutor [" + info.getExecutorId() + "] is started in " + info.getHostname());

		LOG.debug(info.toString());

		conf = new YarnConfiguration();
		FileSystem.get(conf);
		LOG.debug(conf.toString());

		// String zookeeperConnect =
		// System.getenv(MOHA_Properties.CONF_ZOOKEEPER_CONNECT);
		// String bootstrapServer = new
		// MOHA_Zookeeper(MOHA_Properties.ZOOKEEPER_ROOT_KAFKA,
		// info.getConf().getKafkaClusterId()).getBootstrapServers();
		if (queueType.equals("kafka")) {
			jobQueue = new MOHA_Queue(zookeeperConnect, bootstrapServer, info.getQueueName());
		} else {
			jobQueue = new MOHA_Queue(System.getenv(MOHA_Properties.CONF_ACTIVEMQ_SERVER), info.getQueueName());
		}
		// jobQueue = new MOHA_Queue(zookeeperConnect, bootstrapServer,
		// info.getQueueName());

		database = new MOHA_Database(Boolean.parseBoolean(info.getConf().getMysqlLogEnable()));
		LOG.debug(database.toString());
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Container just started on {}" + NetUtils.getHostname());
		try {
			MOHA_TaskExecutor executor = new MOHA_TaskExecutor(args);
			executor.run();
			// executor.KOHA_run();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void KOHA_producer(int num, String msg) {
		/* Creating a queue and pushing jobs to the queue */
		jobQueue.producerInit();
		// add padding
		// LOG.all("TaskExecutor [" + info.getExecutorId() + "] adds padding");

		MOHA_Zookeeper zks = new MOHA_Zookeeper(MOHA_Properties.ZOOKEEPER_ROOT, info.getAppId());
		// ready for pushing
		// for (int i = 0; i < num*1; i++) {
		// jobQueue.push(Integer.toString(i), msg);
		// }
		// zks.setPollingEnable(info.getExecutorId(),
		// MOHA_Properties.TIMMING_PUSHING);

		// LOG.all("TaskExecutor [" + info.getExecutorId() + "] is ready");
		while (zks.getTimming() != MOHA_Properties.TIMMING_PUSHING) {
			// wait for start
			// for (int i = 0; i < num; i++) {
			// jobQueue.push(Integer.toString(i), msg);
			// }
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// calculate
		LOG.all("TaskExecutor [" + info.getExecutorId() + "] starts actual pushing");
		long startTime = System.currentTimeMillis();

		for (int i = 0; i < num; i++) {
			// jobQueue.push(Integer.toString(i), msg);
		}
		long pushingTime = (System.currentTimeMillis() - startTime);
		long rate = (num / pushingTime) * 1000;
		LOG.all("TaskExecutor [" + info.getExecutorId() + "] have pushed " + String.valueOf(num) + " (messages) in " + String.valueOf(pushingTime) + " mini seconds and speed is "
				+ String.valueOf(rate) + " messages/second");
		info.setPushingRate(rate);// messages per second

		// add padding
		// LOG.all("TaskExecutor [" + info.getExecutorId() + "] keeps padding");

		/// zks.setPollingEnable(info.getExecutorId(),
		/// MOHA_Properties.TIMMING_PUSHING_FINISH);

		while (zks.getTimming() != MOHA_Properties.TIMMING_PUSHING_FINISH) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// for (int i = 0; i < num; i++) {
			// jobQueue.push(Integer.toString(i), msg);
			// }
		}

		LOG.all("TaskExecutor [" + info.getExecutorId() + "] producer stops");
	}

	private void KOHA_consumer(int number) {
		long startTime = System.currentTimeMillis();
		long expiredTime = System.currentTimeMillis() + 10 * 1;

		long pollingTime = 0;
		Boolean found = false;
		int num = 0;
		ConsumerRecords<String, String> records;
		MOHA_Zookeeper zks = new MOHA_Zookeeper(MOHA_Properties.ZOOKEEPER_ROOT, info.getAppId());
		num = 0;
		// LOG.all("TaskExecutor [" + info.getExecutorId() + "] adds padding for
		// fetching");
		jobQueue.consumerInit();
		while (zks.getTimming() != MOHA_Properties.TIMMING_FETCHING) {
			for (int i = 0; i < 10; i++) {
				records = jobQueue.poll(100);

				num += records.count();
			}
			if (!found && (num > number)) {
				LOG.all("TaskExecutor [" + info.getExecutorId() + "] got first message");
				// zks.setPollingEnable(info.getExecutorId(),
				// MOHA_Properties.TIMMING_FETCHING);
				found = true;
			}

		}
		// start
		// ......................................................................................................

		num = 0;
		LOG.all("TaskExecutor [" + info.getExecutorId() + "] starts for actual fetching");
		startTime = System.currentTimeMillis();
		while (num < number) {

			records = jobQueue.poll(100);

			if (records.count() > 0) {

				num += records.count();

				// LOG.debug("TaskExecutor [" + info.getExecutorId() + "] got "
				// + String.valueOf(records.count()));
			}

			// for (ConsumerRecord<String, String> record : records) {
			// LOG.debug("TaskExecutor [" + info.getExecutorId() + "]: " +
			// record.toString());
			// }
		}
		pollingTime = System.currentTimeMillis() - startTime;

		if (pollingTime > 0) {
			long rate = (num / pollingTime) * 1000;
			LOG.all("TaskExecutor [" + info.getExecutorId() + "] have fetched " + String.valueOf(num) + " (messages) in " + String.valueOf(pollingTime) + " mini seconds and speed is "
					+ String.valueOf(rate) + " messages/second");
			info.setPollingRate(rate);
		} else {
			LOG.all("TaskExecutor [" + info.getExecutorId() + "] did not get any messages ");
			info.setPollingRate(0);
		}
		info.setNumExecutedTasks(num);

		// FINISH MEASURING

		// zks.setPollingEnable(info.getExecutorId(),
		// MOHA_Properties.TIMMING_FETCHING_FINISH);

		///////////////////////// padding

		while (zks.getTimming() != MOHA_Properties.TIMMING_FETCHING_FINISH) {
			// LOG.debug("TaskExecutor [" + info.getExecutorId() + "] before
			// polling ");
			for (int i = 0; i < 100; i++) {
				records = jobQueue.poll(100);
				num += records.count();
			}

		}
		LOG.all("TaskExecutor [" + info.getExecutorId() + "] consumer stops");
	}

	private void KOHA_run() {
		// String msg = "qwertyuioplkjhgfdsazxcvbnm";
		String msg_unit = "sleep 0";
		String msg = "";
		for (int i = 0; i < 1; i++) {
			msg += msg_unit;
		}

		int num = 500000;

		LOG.all("TaskExecutor [" + info.getExecutorId() + "] starts calling producer: " + msg);
		LOG.all("Length = " + String.valueOf(msg.length()));

		KOHA_producer(num, msg);

		LOG.all("TaskExecutor [" + info.getExecutorId() + "] starts calling consumer");
		// KOHA_consumer(num);
		consumer1();

		// database.insertExecutorInfoToDatabase(info);
		jobQueue.close();
		jobQueue.deleteQueue();
	}

	private void consumer1() {
		// TODO Auto-generated method stub

		long startingTime = System.currentTimeMillis();
		long expiredTime = System.currentTimeMillis() + 10 * 1;
		int numOfCommands = 0;
		int numOfPolls = 0;
		int retries = 20;
		boolean found = false; // get first message from the queue
		info.setEndingTime(startingTime);
		LOG.all(info.toString());
		LOG.all("TaskExecutor [" + info.getExecutorId() + "] starts polling and processing jobs got from the job queue");

		MOHA_Zookeeper zkServer = new MOHA_Zookeeper(MOHA_Properties.ZOOKEEPER_ROOT, info.getAppId());
		// MOHA_Zookeeper zks = new
		// MOHA_Zookeeper(MOHA_Properties.ZOOKEEPER_ROOT_MOHA, info.getAppId());

		LOG.debug(zkServer.toString());
		jobQueue.consumerInit();

		while (System.currentTimeMillis() < expiredTime) {

			ConsumerRecords<String, String> records = jobQueue.poll(100);

			if ((records.count() > 0) && (!found)) {
				info.setFirstMessageTime(System.currentTimeMillis());
				LOG.debug("TaskExecutor [" + info.getExecutorId() + "] got first messages at tries " + (20 - retries));
				retries = 5;
				found = true;
			}

			if ((records.count() > 0) && ((expiredTime - startingTime) < 1)) {

				jobQueue.commitSync();
				numOfPolls++;
				numOfCommands += records.count();
				info.setEndingTime(System.currentTimeMillis());
				// expiredTime = System.currentTimeMillis() +
				// EXTENDED_SESSION_TIME;

				zkServer.setCompletedJobsNum(info.getExecutorId(), numOfCommands);

				// LOG.all("TaskExecutor [" + info.getExecutorId() + "] have
				// completed " + numOfCommands + " jobs");
			} else if (retries > 0) {

				// jobQueue.subcribe();
				// LOG.all("TaskExecutor [" + info.getExecutorId() + "] could no
				// longer get meseages, try to re-poll");
				try {
					Thread.sleep(100);
					// jobQueue.commitSync();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				retries--;
				// expiredTime = System.currentTimeMillis() +
				// EXTENDED_SESSION_TIME;
			}

			if (!found) {
				LOG.debug("TaskExecutor [" + info.getExecutorId() + "] could not found");
			}

			if (retries == 0) {
				// zkServer.setPollingEnable(info.getExecutorId(),
				// MOHA_Properties.TIMMING_FETCHING_FINISH);
			}

			///////////////////////// padding

			if (zkServer.getTimming() != MOHA_Properties.TIMMING_FETCHING_FINISH) {
				expiredTime = System.currentTimeMillis() + 1;
			}

		}
		LOG.all("TaskExecutor [" + info.getExecutorId() + "] have completed " + numOfCommands + " jobs");

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		long executingTime = info.getEndingTime() - info.getFirstMessageTime();
		info.setExecutionTime(executingTime);
		info.setNumExecutedTasks(numOfCommands);
		info.setNumOfPolls(numOfPolls);
		info.setEndingTime(System.currentTimeMillis());
		LOG.debug(info.toString());
		database.insertExecutorInfoToDatabase(info);
		LOG.info(database.toString());
		LOG.all("TaskExecutor [" + info.getExecutorId() + "] exists");
		jobQueue.close();
	}

	public void copyFromHdfs(String source, String dest) throws IOException {

		Configuration conf = new Configuration();

		FileSystem fileSystem = FileSystem.get(conf);
		Path srcPath = new Path(source);

		Path dstPath = new Path(dest);
		LOG.debug("source " + srcPath.toString());
		LOG.debug("dstPath " + dstPath.toString());
		// Check if the file already exists
		if (!(fileSystem.exists(dstPath))) {
			LOG.debug("No such destination " + dstPath);
			return;
		}

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

		try {
			fileSystem.copyToLocalFile(srcPath, dstPath);
			LOG.debug("File " + filename + "copied to " + dest);
		} catch (Exception e) {
			LOG.debug("Exception caught! :" + e);
			System.exit(1);
		} finally {
			fileSystem.close();
		}
	}

	private void docking(ConsumerRecords<String, String> records) {

		Configuration conf = new Configuration();
		FileSystem fileSystem = null;

		try {
			fileSystem = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Path srcPath;
		Path dstPath;

		for (ConsumerRecord<String, String> record : records) {
			LOG.debug("TaskExecutor [" + info.getExecutorId() + "]: " + record.toString() + jobQueue.assignment().toString());
			/* Extract the location of input file in HDFS */
			srcPath = new Path(record.value());
			dstPath = new Path(".");
			// LOG.debug("source " + srcPath.toString());
			// LOG.debug("dstPath " + dstPath.toString());

			try {
				/* Copy tar file from HDFS to local directory */
				long base_ = System.currentTimeMillis();
				fileSystem.copyToLocalFile(srcPath, dstPath);
				LOG.all("copyToLocalFile:" + String.valueOf(System.currentTimeMillis() - base_));

				LOG.debug("source " + srcPath.getName());
				/* Uncompress tar file to tmp folder */
				List<String> uncompressCommand = new ArrayList<String>();
				uncompressCommand.add("tar");
				uncompressCommand.add("-xvzf");
				uncompressCommand.add(srcPath.getName());
				uncompressCommand.add("-C");
				uncompressCommand.add("tmp");
				LOG.debug(uncompressCommand.toString());
				ProcessBuilder builder = new ProcessBuilder(uncompressCommand);
				Process p;
				String cliResponse;

				p = builder.start();
				p.waitFor();
				BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ((cliResponse = br.readLine()) != null) {
					// LOG.debug(line);
				}
				/*
				 * Copy all files from executable directory to current directory
				 */
				File[] listFiles = new File(MOHA_Properties.EXECUTABLE_DIR).listFiles();
				for (File file : listFiles) {
					if (file.isFile()) {
						file.renameTo(new File(file.getName()));
						file.delete();
					}
				}
				/* Get the list of folders, which is the input of tasks */
				File[] listFolders = new File("tmp").listFiles();

				for (File folder : listFolders) {
					Long begin = System.currentTimeMillis();
					if (folder.isDirectory()) {
						LOG.debug("Input directory for current task:" + folder.getName());
						/* Build command to execute the task */
						List<String> taskCommand = new ArrayList<String>();
						taskCommand.add("./autodock_vina.sh");
						taskCommand.add("5-FU");
						taskCommand.add("tmp/" + folder.getName());
						taskCommand.add("scPDB_coordinates.tsv");
						LOG.debug("Command for current task:" + taskCommand.toString());
						builder = new ProcessBuilder(taskCommand);
						/* Execute the task */
						p = builder.start();
						p.waitFor();
						BufferedReader buffReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
						while ((cliResponse = buffReader.readLine()) != null) {
							LOG.debug(cliResponse);
						}
						LOG.all("TaskExecutor [" + info.getExecutorId() + "][" + record.partition() + "] Execution time [" + folder.getName() + "]: "
								+ String.valueOf(System.currentTimeMillis() - begin));
						/*
						 * Delete input folder after the task is completed
						 */
						FileUtil.fullyDelete(folder);
					}
				}
				/* Delete the tar tgz file after uncompressed */
				// fileSystem.delete(new Path(srcPath.getName()), true);
				FileUtil.fullyDelete(new File(srcPath.getName()));
			} catch (IOException | InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			// List<String> command = new ArrayList<String>();
			//
			// String[] str = record.value().split(" ");
			// for (String cmd : str) {
			// command.add(cmd);
			// }
			// //LOG.all("TaskExecutor [" + info.getExecutorId() + "]: " +
			// command.toString());
			// ProcessBuilder builder = new ProcessBuilder(command);
			// Process p;
			// String line;
			// try {
			// p = builder.start();
			// p.waitFor();
			// BufferedReader br = new BufferedReader(new
			// InputStreamReader(p.getInputStream()));
			// while ((line = br.readLine()) != null) {
			// LOG.debug("Task Executor (" + info.getExecutorId() + ") " +
			// "from partition "
			// + record.partition() + " : " + line);
			// }
			// } catch (IOException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// } catch (InterruptedException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
		}

	}

	private boolean executeTask(String record) {

		Configuration conf = new Configuration();
		FileSystem fs = null;
		ProcessBuilder builder;
		Process p;
		String cliResponse;
		boolean isSuccess = true;

		long begin;

		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String hdfsHomeDirectory = System.getenv(MOHA_Properties.HDFS_HOME_DIRECTORY);
		LOG.all("TaskExecutor [" + info.getExecutorId() + "] records:" + record);
		begin = System.currentTimeMillis();
		String[] command_compo = record.split(" ");
		for (int i = 0; i < command_compo.length - 1; i++) {
			// LOG.all(command_compo[i]);
		}
		List<String> taskCommand = new ArrayList<String>();
		String run_command = "./";
		run_command += command_compo[2 * Integer.valueOf(command_compo[0]) + 1];
		taskCommand.add(run_command);

		for (int i = 1; i <= Integer.valueOf(command_compo[0]); i++) {
			taskCommand.add(command_compo[i * 2]);
			if ((command_compo[i * 2 - 1].equals("D")) && (!new File(command_compo[i * 2]).exists())) {
				Path srcPath = new Path(hdfsHomeDirectory, "MOHA/" + info.getAppId() + "/" + command_compo[i * 2]);
				Path dstPath = new Path(command_compo[i * 2]);

				try {
					fs.copyToLocalFile(srcPath, dstPath);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					LOG.all("TaskExecutor [" + info.getExecutorId() + "] copyToLocalFile error: " + e.toString());
				}
			}
		}

		LOG.debug("Command for current task:" + taskCommand.toString());
		builder = new ProcessBuilder(taskCommand);
		/* Execute the task */
		try {
			p = builder.start();
			p.waitFor();
			BufferedReader buffReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((cliResponse = buffReader.readLine()) != null) {
				LOG.debug("TaskExecutor [" + info.getExecutorId() + "]   " + cliResponse);
			}
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.all("TaskExecutor [" + info.getExecutorId() + "] copyToLocalFile error: " + e.toString());
			/*
			 * Copy all files from executable directory to current directory
			 */

			File[] listFiles = new File(MOHA_Properties.EXECUTABLE_DIR).listFiles();
			for (File file : listFiles) {
				if (file.isFile()) {
					file.renameTo(new File(file.getName()));
					// file.delete();
					LOG.debug("TaskExecutor [" + info.getExecutorId() + "] copy file: " + file.getName());
				}
			}

			e.printStackTrace();
			isSuccess = false;
		}

		// File curDir = new File(".");
		// getAllFiles(curDir);
		tempDockingTime = System.currentTimeMillis() - begin;
		LOG.all("TaskExecutor [" + info.getExecutorId() + "][" + record + "] Execution time " + tempDockingTime);
		return isSuccess;
	}

	private void run() {
		// TODO Auto-generated method stub

		long systemCurrentTime;
		int numOfCommands = 0;
		int numOfCommands_pre = 0;
		long capturing_time;
		int numOfPolls = 0;

		boolean found = false; // get first message from the queue
		String logs = "";

		LOG.debug("TaskExecutor [" + info.getExecutorId() + "] starts polling and processing jobs got from the job queue");
		/*
		 * Construct zookeeper server which is used to inform number of completed tasks to MOHA Client
		 */
		MOHA_Zookeeper zkServer = new MOHA_Zookeeper(MOHA_Properties.ZOOKEEPER_ROOT, info.getAppId());

		systemCurrentTime = zkServer.getSystemTime();
		capturing_time = systemCurrentTime;
		/* Set ending time, which can be updated frequently */
		info.setEndingTime(systemCurrentTime);
		info.setFirstMessageTime(systemCurrentTime);
		info.setLaunchedTime(systemCurrentTime);

		zkServer.createExecutorResultsDir(info.getExecutorId());
		String appType = System.getenv(MOHA_Properties.APP_TYPE);

		/*
		 * Copy all files from executable directory to current directory
		 */
		if (appType.equals("S")) {
			File[] listFiles = new File(MOHA_Properties.EXECUTABLE_DIR).listFiles();
			LOG.debug("TaskExecutor [" + info.getExecutorId() + "] number of files: " + listFiles.length);
			// Hadoop resource localization fail
			if (listFiles.length == 0) {
				LOG.all("TaskExecutor [" + info.getExecutorId() + "] is running on " + info.getHostname() + " resource localization fail ");
				String localResource = zkServer.getLocalResource();
				// LOG.debug("TaskExecutor [" + info.getExecutorId() + "] localResource: " + localResource);
				// String hdfsHomeDirectory = System.getenv(MOHA_Properties.HDFS_HOME_DIRECTORY);
				Path srcPath = new Path(localResource);
				Path dstPath = new Path("run.tgz");
				Configuration conf = new Configuration();
				FileSystem fs = null;

				try {
					fs = FileSystem.get(conf);
					fs.copyToLocalFile(srcPath, dstPath);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					LOG.all(" copyToLocalFile error: " + e.toString());
				}
				LOG.debug("TaskExecutor [" + info.getExecutorId() + "] Start uncompressing the tar file");
				getAllFiles(info.getExecutorId(), new File("."));
				/* Uncompress tar file to tmp folder */
				List<String> uncompressCommand = new ArrayList<String>();
				uncompressCommand.add("tar");
				uncompressCommand.add("-xvzf");
				uncompressCommand.add("run.tgz");
				// uncompressCommand.add("-C");
				// uncompressCommand.add(MOHA_Properties.EXECUTABLE_DIR);
				LOG.debug(uncompressCommand.toString());
				ProcessBuilder builder = new ProcessBuilder(uncompressCommand);
				Process p;
				String cliResponse;

				try {
					p = builder.start();
					p.waitFor();
					BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
					while ((cliResponse = br.readLine()) != null) {
						LOG.debug("TaskExecutor [" + info.getExecutorId() + "] local resource files: " + cliResponse);
					}
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				LOG.debug("TaskExecutor [" + info.getExecutorId() + "] after uncompress: ");
				getAllFiles(info.getExecutorId(), new File("."));
			} else {
				LOG.all("TaskExecutor [" + info.getExecutorId() + "] is running on " + info.getHostname() + " resource localization success");
				LOG.debug("TaskExecutor [" + info.getExecutorId() + "] Start copying files from exe to current directory");

				listFiles = new File(MOHA_Properties.EXECUTABLE_DIR).listFiles();
				for (File file : listFiles) {
					if (file.isFile()) {
						file.renameTo(new File(file.getName()));
						// file.delete();
						LOG.debug("TaskExecutor [" + info.getExecutorId() + "] copy file: " + file.getName());
					}
				}
			}
		}

		// Wait until all executors started
		while (!zkServer.getPollingEnable()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				LOG.all(e.toString());
				e.printStackTrace();
			}

		}
		LOG.debug("TaskExecutor [" + info.getExecutorId() + "] SUBCRIBE TO THE JOB QUEUE");
		/* Subscribe to the job queue to be allowed polling task input files */
		jobQueue.consumerInit();
		capturing_time = zkServer.getSystemTime();
		int num = 0;
		boolean isUpdated = false;
		boolean isProcessingSuccess = true;
		/* Polling first jobs from job queue */
		boolean isStop = zkServer.getStopRequest();
		LOG.all("TaskExecutor [" + info.getExecutorId() + "]:" + jobQueue.toString());
		while (!isStop) {
			/* Polling tasks from job queue */
			if (jobQueue.isKafka()) {
				/*
				 * if(numOfCommands <1000000){ num = 1; }else{ num = 0; }
				 */
				ConsumerRecords<String, String> records = jobQueue.poll(100);
				num = records.count();

				if (appType.equals("S")) {
					for (ConsumerRecord<String, String> record : records) {
						isProcessingSuccess = executeTask(record.value());
					}
				} else {
					for (ConsumerRecord<String, String> record : records) {
						// LOG.all("TaskExecutor [" + info.getExecutorId() + "]Received: " + record.value());
					}
				}

			} else {
				Message msg = jobQueue.activeMQPoll(100);
				if (msg != null) {
					if (msg instanceof TextMessage) {
						TextMessage textMessage = (TextMessage) msg;
						String text;
						try {
							text = textMessage.getText();
							if (appType.equals("S")) {
								isProcessingSuccess = executeTask(text);
							}

							// LOG.all("TaskExecutor [" + info.getExecutorId() + "]Received: " + text);
						} catch (JMSException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} else {
						if (appType.equals("S")) {
							isProcessingSuccess = executeTask(msg.toString());
						}
						// LOG.all("TaskExecutor [" + info.getExecutorId() + "]Received: " + msg);
					}
					num = 1;
				} else {
					num = 0;

					if (!found) {
						LOG.all("TaskExecutor [" + info.getExecutorId() + "] does not get any message ");
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

				}
			}
			systemCurrentTime = zkServer.getSystemTime();
			if (num > 0) {
				if (!found) {

					info.setFirstMessageTime(systemCurrentTime);
					info.setWaitingTime(systemCurrentTime - info.getLaunchedTime());

					if (jobQueue.isKafka) {
						LOG.all("TaskExecutor [" + info.getExecutorId() + "]found first package" + "& partitions: " + jobQueue.assignment().toString());
					} else {
						LOG.all("TaskExecutor [" + info.getExecutorId() + "]found first package ActiveMQ");
					}
					capturing_time = systemCurrentTime;
					zkServer.setTimeStart(systemCurrentTime);
					found = true;
				}
				/* Kafka update offset */
				jobQueue.commitSync();
				numOfPolls++;
				numOfCommands += num;
				isUpdated = false;
			} else {
				/* Update ending time */
				if (!isUpdated) {
					info.setEndingTime(systemCurrentTime);
					zkServer.setTimeComplete(systemCurrentTime);
					zkServer.setNumOfProcessedTasks(info.getExecutorId(), numOfCommands);
					isUpdated = true;
					if (found) {
						LOG.all("TaskExecutor [" + info.getExecutorId() + "] the job queue seems to be empty");
					}
				}
				isStop = zkServer.getStopRequest();
				if (isStop)
					LOG.all("TaskExecutor [" + info.getExecutorId() + "] get stop request");
			}
			/* Log performance (tasks/second) */

			if (appType.equals("S")) {
				if (num > 0) {
					logs += info.getAppId() + " " + info.getExecutorId() + " " + MOHA_Common.convertLongToDate(System.currentTimeMillis()) + "Z " + info.getHostname() + " dockingTime: "
							+ tempDockingTime + " at " + String.valueOf(zkServer.getSystemTime()) + "\n";
				}

			} else {
				/* Log data every one second */
				if ((systemCurrentTime - capturing_time) > 1000) {
					double pollingRate = (double) (numOfCommands - numOfCommands_pre) / (double) ((systemCurrentTime - capturing_time) / 1000);

					if (pollingRate > 0) {
						logs += info.getAppId() + " " + info.getExecutorId() + " " + MOHA_Common.convertLongToDate(System.currentTimeMillis()) + "Z " + info.getHostname() + " " + pollingRate + " "
								+ String.valueOf(zkServer.getSystemTime()) + "\n";
						numOfCommands_pre = numOfCommands;
						capturing_time = systemCurrentTime;
					}

				}
			}

			/* if timeout set stop request and break out */
			if ((systemCurrentTime - info.getLaunchedTime()) > MOHA_Properties.SESSION_MAXTIME_TIMEOUT) {
				zkServer.setTimeComplete(zkServer.getSystemTime());
				zkServer.setNumOfProcessedTasks(info.getExecutorId(), numOfCommands);
				zkServer.setStopRequest(true);
				LOG.all("TaskExecutor [" + info.getExecutorId() + "] timeout");
				break;
			}

			if (!isProcessingSuccess) {
				zkServer.setTimeComplete(zkServer.getSystemTime());
				zkServer.setNumOfProcessedTasks(info.getExecutorId(), numOfCommands);
				LOG.all("TaskExecutor [" + info.getExecutorId() + "] get errors during execution");
				break;
			}

		}

		/* Display list of files and directories for debugging */
		// curDir = new File(".");
		// getAllFiles(curDir);

		LOG.all("TaskExecutor [" + info.getExecutorId() + "] have completed " + numOfCommands + " tasks");
		systemCurrentTime = zkServer.getSystemTime();
		info.setEndingTime(systemCurrentTime);
		long executingTime = info.getEndingTime() - info.getFirstMessageTime();
		info.setExecutionTime(executingTime);
		info.setNumExecutedTasks(numOfCommands);
		info.setNumOfPolls(numOfPolls);

		info.setPollingRate((info.getNumExecutedTasks() / (info.getExecutionTime() / 1000)));

		LOG.debug(info.toString());
		database.insertExecutorInfoToDatabase(info);
		zkServer.setResultsExe(info);
		zkServer.setPerformanceExe(info.getExecutorId(), logs);
		zkServer.close();
		LOG.info(database.toString());
		LOG.all("TaskExecutor [" + info.getExecutorId() + "] exits");
		jobQueue.close();
	}

	protected class ThreadTaskExecutor implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			LOG.debug("TaskExecutor [" + info.getExecutorId() + "] starts polling and processing jobs got from the job queue");
			/*
			 * Construct zookeeper server which is used to inform number of completed tasks to MOHA Client
			 */						
			String appType = System.getenv(MOHA_Properties.APP_TYPE);

			LOG.debug("TaskExecutor [" + info.getExecutorId() + "] SUBCRIBE TO THE JOB QUEUE");
			/* Subscribe to the job queue to be allowed polling task input files */
			jobQueue.consumerInit();			

			/* Polling first jobs from job queue */
			LOG.all("TaskExecutor [" + info.getExecutorId() + "]:" + jobQueue.toString());
			
			boolean isProcessingSuccess = false;
			int num = 0;
			while (info.getNumRunningThreads() <= info.getNumRequestedThreads()) {
				/* Polling tasks from job queue */
				if (jobQueue.isKafka()) {
					/*
					 * if(numOfCommands <1000000){ num = 1; }else{ num = 0; }
					 */
					ConsumerRecords<String, String> records = jobQueue.poll(100);
					num = records.count();

					if (appType.equals("S")) {
						for (ConsumerRecord<String, String> record : records) {
							isProcessingSuccess = executeTask(record.value());
						}
					} else {
						for (ConsumerRecord<String, String> record : records) {
							// LOG.all("TaskExecutor [" + info.getExecutorId() + "]Received: " + record.value());
						}
					}

				} else {
					Message msg = jobQueue.activeMQPoll(100);
					if (msg != null) {
						if (msg instanceof TextMessage) {
							TextMessage textMessage = (TextMessage) msg;
							String text;
							try {
								text = textMessage.getText();
								if (appType.equals("S")) {
									isProcessingSuccess = executeTask(text);
								}

								// LOG.all("TaskExecutor [" + info.getExecutorId() + "]Received: " + text);
							} catch (JMSException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} else {
							if (appType.equals("S")) {
								isProcessingSuccess = executeTask(msg.toString());
							}
							// LOG.all("TaskExecutor [" + info.getExecutorId() + "]Received: " + msg);
						}
						num = 1;
					} else {
						num = 0;
						break;
					}
				}				
				info.setNumExecutedTasks(info.getNumExecutedTasks() + num);
				if(!isProcessingSuccess)break;
			}
			
			info.setNumRunningThreads(info.getNumRunningThreads() - 1);
		}

	}

	private static void getAllFiles(int executorId, File curDir) {

		File[] filesList = curDir.listFiles();

		for (File f : filesList) {
			if (f.isDirectory()) {
				LOG.debug("TaskExecutor [" + executorId + "] Directory " + f.getName());
				getAllFiles(executorId, f);
			}
			if (f.isFile()) {
				LOG.debug("TaskExecutor [" + executorId + "] Files: " + curDir.getPath() + "/" + f.getName());
			}
		}

	}

	/**
	 * @return The line number of the code that ran this method
	 * @author Brian_Entei
	 */
	public static int getLineNumber() {
		return ___8drrd3148796d_Xaf();
	}

	/**
	 * This methods name is ridiculous on purpose to prevent any other method names in the stack trace from potentially matching this one.
	 * 
	 * @return The line number of the code that called the method that called this method(Should only be called by getLineNumber()).
	 * @author Brian_Entei
	 */
	private static int ___8drrd3148796d_Xaf() {
		boolean thisOne = false;
		int thisOneCountDown = 1;
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		for (StackTraceElement element : elements) {
			String methodName = element.getMethodName();
			int lineNum = element.getLineNumber();
			if (thisOne && (thisOneCountDown == 0)) {
				return lineNum;
			} else if (thisOne) {
				thisOneCountDown--;
			}
			if (methodName.equals("___8drrd3148796d_Xaf")) {
				thisOne = true;
			}
		}
		return -1;
	}
}
