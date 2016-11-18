package org.kisti.moha;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MOHA_KafkaManager {
	public class RMCallbackHandler implements CallbackHandler {

		@Override
		public void onContainersCompleted(List<ContainerStatus> statuses) {
			// TODO Auto-generated method stub
			LOG.info("Got response from RM for container ask, completed count = {}", statuses.size());
			for (ContainerStatus status : statuses) {
				numCompletedContainers.incrementAndGet();
				LOG.info("Container completed: ", status.getContainerId());
			}
		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			// TODO Auto-generated method stub
			LOG.info("Got response from RM for container ask, allocated count = {}", containers.size());
			for (Container container : containers) {

				ContainerLauncher launcher = new ContainerLauncher(container, numAllocatedContainers.getAndIncrement(),
						containerListener);
				Thread mhmThread = new Thread(launcher);
				mhmThread.start();
				launchThreads.add(mhmThread);

			}
		}

		@Override
		public void onShutdownRequest() {
			// TODO Auto-generated method stub
			done = true;
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {
			// TODO Auto-generated method stub

		}

		@Override
		public float getProgress() {
			// TODO Auto-generated method stub
			float progress = numOfContainers <= 0 ? 0 : (float) numCompletedContainers.get() / numOfContainers;
			return progress;
		}

		@Override
		public void onError(Throwable e) {
			// TODO Auto-generated method stub
			done = true;
			amRMClient.stop();
		}

	}

	private static final Logger LOG = LoggerFactory.getLogger(MOHA_KafkaManager.class);
	private YarnConfiguration conf;
	private AMRMClientAsync<ContainerRequest> amRMClient;

	private FileSystem fileSystem;
	private int numOfContainers;
	protected AtomicInteger numCompletedContainers = new AtomicInteger();
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();
	private volatile boolean done;
	protected NMClientAsync nmClient;
	private KBCallbackHandler containerListener;
	private List<Thread> launchThreads = new ArrayList<>();
	
	private static MOHA_Logger debugLogger;
	
	private MOHA_KafkaInfo kafkaInfo;
	Vector<CharSequence> statistic = new Vector<>(30);

	public MOHA_KafkaManager(String[] args) throws IOException {
		debugLogger = new MOHA_Logger();
		conf = new YarnConfiguration();
		fileSystem = FileSystem.get(conf);
		for (String str : args) {
			LOG.info(str);
		}
		
		kafkaInfo = new MOHA_KafkaInfo();
		
		kafkaInfo.setKafkaClusterName(args[0]);
		kafkaInfo.setBrokerMem(Integer.parseInt(args[1]));
		kafkaInfo.setNumBrokers(Integer.parseInt(args[2]));
		kafkaInfo.setNumPartitions(kafkaInfo.getNumBrokers());
		kafkaInfo.setLibsPath(args[3]);
		kafkaInfo.setStartingTime(Long.parseLong(args[4]));
		kafkaInfo.setKafkaClusterId(args[5]);
		
		LOG.info("queue name = {}, executor memory = {}, num executors = {}, jdlPath = {}", kafkaInfo.getKafkaClusterName(),
				kafkaInfo.getBrokerMem(), kafkaInfo.getNumBrokers(), kafkaInfo.getLibsPath());

		
		String ipAddress = InetAddress.getLocalHost().getHostAddress();
		LOG.info("Host idAdress = {}", ipAddress);

				


	}



	public void run() throws YarnException, IOException {
		LOG.info(debugLogger.info("MOHA_KafkaManager is to be running ..."));

		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
		amRMClient.init(conf);
		LOG.info(debugLogger.info("amRMClient.start() ..."));
		amRMClient.start();
		RegisterApplicationMasterResponse response;
		response = amRMClient.registerApplicationMaster(NetUtils.getHostname(), -1, "");
		LOG.info("MOHA Manager is registered with response : {}", response.toString());

		LOG.info(debugLogger.info("nmClient.start(); ..."));

		containerListener = new KBCallbackHandler(this);
		nmClient = NMClientAsync.createNMClientAsync(containerListener);
		nmClient.init(conf);
		nmClient.start();

		kafkaInfo.setAllocationTime(System.currentTimeMillis());
		// request resources to launch containers
		Resource capacity = Records.newRecord(Resource.class);
		capacity.setMemory(kafkaInfo.getBrokerMem());
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(0);

		for (int i = 0; i < kafkaInfo.getNumBrokers(); i++) {
			LOG.info(debugLogger.info("Request containers from Resourse Manager, containerNumber = " + i));
			ContainerRequest containerRequest = new ContainerRequest(capacity, null, null, pri);
			amRMClient.addContainerRequest(containerRequest);
			numOfContainers++;
		}
		
		try {
			Thread.sleep(1000);

		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (!done && (numCompletedContainers.get() < numOfContainers)) {

			try {
				// LOG.info(outputQueue.push("The number of completed Containers
				// = " + this.numCompletedContainers.get()));
				Thread.sleep(1000);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		LOG.info(debugLogger.info("The number of completed Containers = " + this.numCompletedContainers.get()));
		LOG.info(debugLogger.info("Containers have all completed, so shutting down NMClient and AMRMClient ..."));

		kafkaInfo.setMakespan(System.currentTimeMillis() - kafkaInfo.getStartingTime());debugLogger.info("setMakespan");
		
		MOHA_Zookeeper zk = new MOHA_Zookeeper(kafkaInfo.getKafkaClusterId());
		zk.delete();
		zk.close();
		
		debugLogger.info(kafkaInfo.getKafkaClusterId());
		nmClient.stop();debugLogger.info("stop");
		amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Application complete!", null);debugLogger.info("unregisterApplicationMaster");
		amRMClient.stop();debugLogger.info("stop");
		

	}



	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LOG.info("Starting MOHA Manager ...");
		try {
			MOHA_KafkaManager mhm = new MOHA_KafkaManager(args);
			mhm.run();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	protected class ContainerLauncher implements Runnable {
		private Container container;
		@SuppressWarnings("unused")
		private KBCallbackHandler containerListener;
		private int id;

		public ContainerLauncher(Container container, int id, KBCallbackHandler containerListener) {
			super();
			this.container = container;
			this.containerListener = containerListener;
			this.id = id;

			LOG.info(containerListener.toString());
		}

		public String getLaunchCommand(Container container, int id) {
			Vector<CharSequence> vargs = new Vector<>(30);
			vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
			vargs.add(MOHA_KafkaBrokerLauncher.class.getName());
			vargs.add(kafkaInfo.getKafkaClusterId());
			vargs.add(container.getId().toString());
			vargs.add(String.valueOf(id));
			vargs.add(kafkaInfo.getLibsPath().replace(".tgz", ""));
			vargs.add("1><LOG_DIR>/MOHA_KafkaBrokerLauncher.stdout");
			vargs.add("2><LOG_DIR>/MOHA_KafkaBrokerLauncher.stderr");
			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}
			return command.toString();
		}

		@Override
		public void run() {

			LOG.info("Setting up ContainerLauncher for containerid = {}", container.getId());
			Map<String, LocalResource> localResources = new HashMap<>();
			Map<String, String> env = System.getenv();
			LocalResource appJarFile = Records.newRecord(LocalResource.class);
			appJarFile.setType(LocalResourceType.FILE);
			appJarFile.setVisibility(LocalResourceVisibility.APPLICATION);
			try {
				appJarFile.setResource(ConverterUtils.getYarnUrlFromURI(new URI(env.get("AMJAR"))));
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			appJarFile.setTimestamp(Long.valueOf((env.get("AMJARTIMESTAMP"))));
			appJarFile.setSize(Long.valueOf(env.get("AMJARLEN")));
			localResources.put("app.jar", appJarFile);
			LOG.info("Added {} as a local resource to the Container ", appJarFile.toString());

			// Setting Kafka dependencies package
			LocalResource kafkaPackage = Records.newRecord(LocalResource.class);
			kafkaPackage.setType(LocalResourceType.ARCHIVE);
			kafkaPackage.setVisibility(LocalResourceVisibility.APPLICATION);
			try {
				kafkaPackage.setResource(ConverterUtils.getYarnUrlFromURI(new URI(env.get("KAFKALIBS"))));
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			kafkaPackage.setTimestamp(Long.valueOf((env.get("KAFKALIBSTIMESTAMP"))));
			kafkaPackage.setSize(Long.valueOf(env.get("KAFKALIBSLEN")));
			localResources.put("kafkaLibs", kafkaPackage);

			LOG.info("Added {} as a local resource to the Container ", kafkaPackage.toString());

			ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
			context.setEnvironment(env);
			context.setLocalResources(localResources);

			String command = getLaunchCommand(container, this.id);
			List<String> commands = new ArrayList<>();
			commands.add(command);
			context.setCommands(commands);
			LOG.info("Command to execute MOHA_KafkaBrokerLauncher = {}", command);
			nmClient.startContainerAsync(container, context);
			LOG.info("Container {} launched!", container.getId());
		}
	}

}
