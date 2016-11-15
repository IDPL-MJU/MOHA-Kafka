package org.kisti.moha;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MOHA_KafkaClient {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_KafkaClient.class);
	private YarnConfiguration conf;
	private YarnClient yarnClient;
	private ApplicationId appId;
	private FileSystem fs;

	private String appName;
	private int priority;
	private String queue;
	private int managerMemory;
	private String jarPath;
	private int brokerMem;
	private int numBrokers;
	private String kafkaLibsPath;
	private long startingTime;

	
	public static void main(String[] args) throws IOException {	
		//[UPDATE] Change the flow of the main function to enable exception handling at each step
		/*
		MOHA_KafkaClient client = new MOHA_KafkaClient(args);
		
		try {
			boolean result = client.run();
			LOG.info(String.valueOf(result));
		} catch (YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		MOHA_KafkaClient client;
		boolean result = false;
		
		LOG.info("Initializing the MOHA_KafkaClient");

		try {
			client = new MOHA_KafkaClient(args);
			result = client.init(args);
			
			if(!result) {
				LOG.info("Finishing the Kafka Cluster without YARN submission ...");
				return;
			}
			
			result = client.run();
		} catch (IOException | ParseException | YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
		
		if(result) {
			LOG.info("The MOHA_KafkaClient is successfully executed");
		}
	}//The end of main function

	
	public MOHA_KafkaClient(String[] args) throws IOException {
		//[UPDATE] Some logics are shifted into the main function
		/*
		try {
			LOG.info("Start init MOHA_KafkaClient");
			startingTime = System.currentTimeMillis();
			init(args);
			LOG.info("Successfully init");
			conf = new YarnConfiguration();
			yarnClient = YarnClient.createYarnClient();
			yarnClient.init(conf);
			fs = FileSystem.get(conf);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		conf = new YarnConfiguration();
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		fs = FileSystem.get(conf);
	}//The end of MOHA_KafkaClient constructor
	
	
	public boolean init(String[] args) throws ParseException {
		/* 
		 * Add an option that only contains a short-name. It may be specified as requiring an argument.
		   - Parameters
		     : opt (Short single-character name of the option)
		     : hasArg flag (signally if an argument is required after this option)
		     : description (Self-documenting description)
		   - Returns: the resulting Options instance
		 */
		//[UPDATE] change the hadArg flags into "true" except for the help option
		Options option = new Options();
		option.addOption("appname", true, "MOHA Application Name (Default: MOHA)");
		option.addOption("priority", true, "Application Priority (Default: 0)");
		option.addOption("queue", true,
				"RM Queue in which this application is to be submitted (Default: default)");
		option.addOption("manager_memory", true, 
				"Amount of memory in MB to be requested to run the MOHA Manager (Default: 1024)");
		option.addOption("jar", true,
				"JAR file containing the MOHA Manager and Task Executor (Default: MOHA.jar)");
		option.addOption("executor_memory", true,
				"Amount of memory in MB to be requested to run the MOHA TaskExecutor (Default: 1024)");
		option.addOption("num_executors", true, "Number of MOHA Task Executors (Default: 1)");
		option.addOption("JDL", true, "Job Description Language file that contains the MOHA job specification (must specified)");		
		option.addOption("help", false, "Print Usage of MOHA_KafkaClient"); //Add the help functionality in MOHA_KafkaClient

		CommandLine inputParser = new GnuParser().parse(option, args);
		
		//[UPDATE] Add the help functionality in MOHA_KafkaClient
		if (inputParser.hasOption("help")) {
			printUsage(option);
			return false;
		}
		
		//[UPDATE] Add default values for options
		appName = inputParser.getOptionValue("appname", "MOHA");
		priority = Integer.parseInt(inputParser.getOptionValue("priority", "0"));
		queue = inputParser.getOptionValue("queue", "default");
		managerMemory = Integer.parseInt(inputParser.getOptionValue("manager_memory", "1024"));
		jarPath = inputParser.getOptionValue("jar", "MOHA.jar");
		brokerMem = Integer.parseInt(inputParser.getOptionValue("executor_memory", "1024"));
		numBrokers = Integer.parseInt(inputParser.getOptionValue("num_executors", "1"));
		
		//[UPDATE] The Job Description File is necessary to execute MOHA tasks
		if(!inputParser.hasOption("JDL")) {
			LOG.error("The Job Description File should be provided !");
			return false;
		}
		kafkaLibsPath = inputParser.getOptionValue("JDL");

		//[UPDATE] change the exception handling logic to avoid unnecessary exception throwing
		if (priority < 0) {
			LOG.error("Invalid value is specified for the Application Priority");
			return false;
			//throw new IllegalArgumentException("Invalid value specified for Application Priority");
		}
		
		//[UPDATE] Unless there is a minimum memory requirement, positive values look O.K.
		//if (managerMemory < 32) {
		if (managerMemory <= 0) {
			LOG.error("Invalid value is specified for the amount of memory of the MOHA Manager");
			return false;
			//throw new IllegalArgumentException(
			//		"Invalid value specified for amout of memory in MB to be requested to run the MOHA Manager");
		}
		
		//if (executorMemory < 32) {
		if (brokerMem <= 0) {
			LOG.error("Invalid value is specified for the amount of memory of the MOHA TaskExecutor");
			return false;
			//throw new IllegalArgumentException(
			//		"Invalid value specified for amount of memory in MB to be requested to run the MOHA TaskExecutor");
		}
		
		if (numBrokers < 1) {
			LOG.error("Invalid value is specified for the number of MOHA TaskExecutors");
			return false;
			//throw new IllegalArgumentException(
			//		"Invalid value specified for number of MOHA TaskEcecutor to be executed");
		}
		
		LOG.info("App name = {}, priority = {}, queue = {}, manager memory = {}, jarPath = {}, executor memory = {}, "
				+ "num ececutors = {}, jdl path = {}", appName, priority, queue, managerMemory, jarPath, brokerMem, 
				numBrokers, kafkaLibsPath);

		return true;
	}//The end of init function
	
	
	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("MOHA_KafkaClient", opts);
	}
	

	public boolean run() throws YarnException, IOException {
		LOG.info("yarnClient = {}", yarnClient.toString());
		yarnClient.start();
		YarnClientApplication yarnClientApplication = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = yarnClientApplication.getNewApplicationResponse();
		appId = appResponse.getApplicationId();
		
		LOG.info("Application ID = {}", appId);
		int maxMemory = appResponse.getMaximumResourceCapability().getMemory();
		if (managerMemory > (maxMemory)) {
			managerMemory = maxMemory;
		}
		int maxVcores = appResponse.getMaximumResourceCapability().getVirtualCores();
		LOG.info("Max memory = {} and max vcores = {}", maxMemory, maxVcores);
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
		LOG.info("Number of NodeManagers in the Cluster = {}", clusterMetrics.getNumNodeManagers());
		List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
		for (NodeReport node : nodeReports) {
			LOG.info("Node ID = {} , address = {}, container = {}", node.getNodeId(), node.getHttpAddress(),
					node.getNumContainers());

		}
		List<QueueInfo> nodeQueues = yarnClient.getAllQueues();
		for (QueueInfo queues : nodeQueues) {
			LOG.info("name = {}, capacity = {}, maximum capacity of each queue = {}", queues.getQueueName(),
					queues.getCapacity(), queues.getMaximumCapacity());
		}
		Path src = new Path(this.jarPath);
		String pathSuffix = appName + "/" + appId.getId() + "/app.jar";
		Path dest = new Path(fs.getHomeDirectory(), pathSuffix);
		fs.copyFromLocalFile(false, true, src, dest);
		FileStatus destStatus = fs.getFileStatus(dest);

		LocalResource jarResource = Records.newRecord(LocalResource.class);
		jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dest));
		jarResource.setSize(destStatus.getLen());
		jarResource.setTimestamp(destStatus.getModificationTime());
		jarResource.setType(LocalResourceType.FILE);
		jarResource.setVisibility(LocalResourceVisibility.APPLICATION);
		Map<String, LocalResource> localResources = new HashMap<>();
		localResources.put("app.jar", jarResource);
		LOG.info("Jar resource = {}", jarResource.toString());

		Path jdlsrc = new Path(this.kafkaLibsPath);
		String pathSuffixJdl = appName + "/" + appId.getId() + "/" + MOHA_Properties.kafkaLibs;
		Path destKafka = new Path(fs.getHomeDirectory(), pathSuffixJdl);
		fs.copyFromLocalFile(false, true, jdlsrc, destKafka);
		
		
		FileStatus kafkaStatus = fs.getFileLinkStatus(destKafka);
		//set local resource
		/*LocalResource kafkaResource = Records.newRecord(LocalResource.class);
		kafkaResource.setResource(ConverterUtils.getYarnUrlFromPath(destKafka));
		kafkaResource.setSize(kafkaStatus.getLen());
		kafkaResource.setTimestamp(kafkaStatus.getModificationTime());
		kafkaResource.setType(LocalResourceType.FILE);
		kafkaResource.setVisibility(LocalResourceVisibility.APPLICATION);

		localResources.put(MOHA_Properties.kafkaLibs, kafkaResource);*/
		//LOG.info("Jdl resource = {}", kafkaResource.toString());

		Map<String, String> env = new HashMap<>();
		String appJarDest = dest.toUri().toString();
		
		env.put("AMJAR", appJarDest);
		env.put("AMJARTIMESTAMP", Long.toString(destStatus.getModificationTime()));
		env.put("AMJARLEN", Long.toString(destStatus.getLen()));
		
		env.put("KAFKALIBS", destKafka.toUri().toString());
		env.put("KAFKALIBSTIMESTAMP", Long.toString(kafkaStatus.getModificationTime()));
		env.put("KAFKALIBSLEN", Long.toString(kafkaStatus.getLen()));

		StringBuilder classPathEnv = new StringBuilder().append(File.pathSeparatorChar).append("./app.jar");
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(c.trim());
		}
		
		Properties prop = new Properties();
		/* Loading MOHA.Conf File */
		String kafka_libs ="default";
		try {
			prop.load(new FileInputStream("conf/MOHA.conf"));
			kafka_libs = prop.getProperty("MOHA.dependencies.kafka.libs");
			System.out.println(kafka_libs);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		classPathEnv.append(File.pathSeparatorChar);
		classPathEnv.append(kafka_libs);
		classPathEnv.append(File.pathSeparatorChar);
		classPathEnv.append(Environment.CLASSPATH.$());
		env.put("CLASSPATH", classPathEnv.toString());
		LOG.info("Classpath = {}", classPathEnv.toString());
		ApplicationSubmissionContext appContext = yarnClientApplication.getApplicationSubmissionContext();
		appContext.setApplicationName(appName);

		ContainerLaunchContext mhmContainer = Records.newRecord(ContainerLaunchContext.class);
		LOG.info("Local resources = {}", localResources.toString());
		mhmContainer.setLocalResources(localResources);
		mhmContainer.setEnvironment(env);

		Vector<CharSequence> vargs = new Vector<>();
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

		vargs.add(MOHA_KafkaManager.class.getName());
		vargs.add(appId.toString());
		vargs.add(String.valueOf(brokerMem));
		vargs.add(String.valueOf(numBrokers));
		vargs.add(MOHA_Properties.kafkaLibs);
		vargs.add(String.valueOf(startingTime));
		vargs.add("1><LOG_DIR>/MOHA_KafkaManager.stdout");
		vargs.add("2><LOG_DIR>/MOHA_KafkaManager.stderr");
		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}
		List<String> commands = new ArrayList<>();
		commands.add(command.toString());

		LOG.info("Command to execute MOHA Manager = {}", command);

		mhmContainer.setCommands(commands);

		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(managerMemory);
		appContext.setResource(capability);
		appContext.setAMContainerSpec(mhmContainer);

		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(priority);
		appContext.setPriority(pri);
		appContext.setQueue(queue);

		LOG.info("MOHA Manager Container = {}", mhmContainer.toString());
		yarnClient.submitApplication(appContext);

		return true;
	}//The end of run function

}//The end of MOHA_KafkaClient class
