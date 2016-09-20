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

public class MOHA_Client {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_Client.class);
	// private static final String APP_NAME = "MOHA";
	private YarnConfiguration conf;
	private YarnClient yarnClient;
	// private String appJar = "MOHA.jar";
	private ApplicationId appId;
	private FileSystem fs;

	private String appName;
	private int priority;
	private String queue;
	private int managerMemory;
	private String jarPath;
	private int executorMemory;
	private int numExecutors;
	private String jdlPath;
	private long startingTime;

	public MOHA_Client(String[] args) throws IOException {
		try {
			LOG.info("Start init MOHA_Client");
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

	}

	public boolean init(String[] args) throws ParseException {
		Options option = new Options();
		option.addOption("appname", false, "Application Name. Default value - MOHA");
		option.addOption("priority", false, "Application Priority. Default value - 0");
		option.addOption("queue", false,
				"RM Queue in which this application is to be submitted. Default value - default");
		option.addOption("manager_memory", true, "Amout of memory in MB to be requested to run the MOHA Manager");
		option.addOption("jar", false,
				"JAR file containing the MOHA Manager and Task Executor. Default value - ./MOHA.jar");
		option.addOption("executor_memory", true,
				"Amount of memory in MB to be requested to run the MOHA TaskExecutor");
		option.addOption("num_executors", true, "Number of MOHA TaskEcecutor to be executed");
		option.addOption("JDL", true, "Job Description Language file that contains the MOHA job specification");

		CommandLine inputParser = new GnuParser().parse(option, args);
		appName = inputParser.getOptionValue("appname", "MOHA");

		priority = Integer.parseInt(inputParser.getOptionValue("priority", "0"));
		queue = inputParser.getOptionValue("queue", "default");
		managerMemory = Integer.parseInt(inputParser.getOptionValue("manager_memory"));
		jarPath = inputParser.getOptionValue("jar", "MOHA.jar");
		executorMemory = Integer.parseInt(inputParser.getOptionValue("executor_memory"));
		numExecutors = Integer.parseInt(inputParser.getOptionValue("num_executors"));
		jdlPath = inputParser.getOptionValue("JDL");

		if (priority < 0) {
			throw new IllegalArgumentException("Invalid value specified for Application Priority");
		}
		if (managerMemory < 32) {
			throw new IllegalArgumentException(
					"Invalid value specified for amout of memory in MB to be requested to run the MOHA Manager");
		}
		if (executorMemory < 32) {
			throw new IllegalArgumentException(
					"Invalid value specified for amount of memory in MB to be requested to run the MOHA TaskExecutor");
		}
		if (numExecutors < 1) {
			throw new IllegalArgumentException(
					"Invalid value specified for number of MOHA TaskEcecutor to be executed");
		}
		LOG.info(
				"App name = {}, priority = {}, queue = {}, manager memory = {}, jarPath = {}, executor memory = {}, num ececutors = {}, jdl path = {}",
				appName, priority, queue, managerMemory, jarPath, executorMemory, numExecutors, jdlPath);
//		KafkaProducer<Integer, String> producer;
//		Properties props = new Properties();
//		props.put("bootstrap.servers", "localhost:9092");
//		props.put("client.id", "test");
//		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		producer = new KafkaProducer<Integer, String>(props);
//		producer.send(new ProducerRecord<Integer, String>("test", 1, "Successfully init MOHA_Client"));
		return true;
	}

	public boolean run() throws YarnException, IOException {

		LOG.info("yarnClient = {}", yarnClient.toString());
		yarnClient.start();
		YarnClientApplication yarnClientApplication = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = yarnClientApplication.getNewApplicationResponse();
		appId = appResponse.getApplicationId();
		
		LOG.info("Application ID = {}", appId);
		int maxMemory = appResponse.getMaximumResourceCapability().getMemory();
		if (managerMemory > (maxMemory / 2)) {
			managerMemory = maxMemory / 2;
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

		Path jdlsrc = new Path(this.jdlPath);
		String pathSuffixJdl = appName + "/" + appId.getId() + "/" + MOHA_Properties.jdl;
		Path destJdl = new Path(fs.getHomeDirectory(), pathSuffixJdl);
		fs.copyFromLocalFile(false, true, jdlsrc, destJdl);
		FileStatus jdlStatus = fs.getFileLinkStatus(destJdl);
		LocalResource jdlResource = Records.newRecord(LocalResource.class);
		jdlResource.setResource(ConverterUtils.getYarnUrlFromPath(destJdl));
		jdlResource.setSize(jdlStatus.getLen());
		jdlResource.setTimestamp(jdlStatus.getModificationTime());
		jdlResource.setType(LocalResourceType.FILE);
		jdlResource.setVisibility(LocalResourceVisibility.APPLICATION);

		localResources.put(MOHA_Properties.jdl, jdlResource);
		LOG.info("Jdl resource = {}", jdlResource.toString());

		Map<String, String> env = new HashMap<>();
		String appJarDest = dest.toUri().toString();
		env.put("AMJAR", appJarDest);
		LOG.info("AMJAR environment variable is set to {}", appJarDest);
		env.put("AMJARTIMESTAMP", Long.toString(destStatus.getModificationTime()));
		env.put("AMJARLEN", Long.toString(destStatus.getLen()));

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

		vargs.add(MOHA_Manager.class.getName());
		vargs.add(appId.toString());
		vargs.add(String.valueOf(executorMemory));
		vargs.add(String.valueOf(numExecutors));
		vargs.add(MOHA_Properties.jdl);
		vargs.add(String.valueOf(startingTime));
		vargs.add("1><LOG_DIR>/AppMaster.stdout");
		vargs.add("2><LOG_DIR>/AppMaster.stderr");
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
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		MOHA_Client client = new MOHA_Client(args);
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

	}

}
