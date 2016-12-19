package org.kisti.moha;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;

public class MOHA_KafkaBrokerLauncher {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_KafkaBrokerLauncher.class);
	private MOHA_KafkaBrokerInfo kbInfo;
	private static MOHA_Logger debugLogger;
	private static MOHA_Zookeeper zk;

	public MOHA_KafkaBrokerLauncher(String[] args) {
		LOG.info("Container just started on {}" + NetUtils.getHostname());

		kbInfo = new MOHA_KafkaBrokerInfo();

		kbInfo.setContainerId(args[0]);
		kbInfo.setBrokerId(args[1]);

		kbInfo.setKafkaBinDir(kbInfo.getConf().getKafkaVersion());
		kbInfo.setZookeeperConnect(kbInfo.getConf().getZookeeperConnect() + "/" + kbInfo.getConf().getKafkaClusterId());

		debugLogger = new MOHA_Logger((Boolean.parseBoolean(kbInfo.getConf().getKafkaDebugEnable())),
				kbInfo.getConf().getDebugQueueName());

		LOG.info(debugLogger.info("Start MOHA_KafkaBrokerLauncher constructor on " + NetUtils.getHostname()));
		LOG.info(debugLogger.info("Init Kafka broker manager =" + args.toString()));
		LOG.info(debugLogger.info(kbInfo.getConf().toString()));

	}

	public void run() throws IOException, InterruptedException, KeeperException {
		LOG.info("It's about to start Kafka brokers");

		zk = new MOHA_Zookeeper(kbInfo.getConf().getKafkaClusterId());
		if (zk == null) {
			LOG.info("Zookeeper faillllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll");
			LOG.info(kbInfo.getConf().getKafkaClusterId());
			return;
		}

		List<Integer> preIds;
		List<Integer> brokerIds;

		debugLogger.info("It's about to start Kafka brokers");
		preIds = getCurrentKafkaProcess();
		for (int i = 0; i < preIds.size(); i++) {
			LOG.info(preIds.get(i).toString());
			debugLogger.info("preIds = " + preIds.get(i).toString());
		}

		ThreadKafkaStart kafkaStart = new ThreadKafkaStart();
		Thread startThread = new Thread(kafkaStart);

		startThread.start();
		debugLogger.info("startThread.start()");
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		brokerIds = getKafkaBrokerIds(preIds);
		if (!zk.exist()) {
			debugLogger.info(MOHA_Common.convertLongToDate(System.currentTimeMillis())
					+ "  Kafka broker crashed or haven't worked yet ");
			Thread.sleep(1000);
		}

		zk.setRequests(false);
		debugLogger.info(MOHA_Common.convertLongToDate(System.currentTimeMillis()) + "  Kafka brokers are running on "
				+ zk.getBootstrapServers());
		LOG.info("Kafka server is running = {}", zk.getBootstrapServers()); //
		while (!zk.checkRequests()) {

			try {
				Thread.sleep(1000);
				// setRequests(true);
			} catch (InterruptedException e) { // TODO
				// Auto-generated catch block e.printStackTrace();
			}
		}

		kafkaServerStop(brokerIds);
		zk.close();
		LOG.info(debugLogger.info("Kafka broker[" + brokerIds + "] is stopped"));
		return;
	};

	public static void main(String[] args) throws KeeperException, IOException {

		// List<Integer> ids;
		MOHA_KafkaBrokerLauncher brokerManager = new MOHA_KafkaBrokerLauncher(args);

		try {
			brokerManager.run();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	/**
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */

	private List<Integer> getCurrentKafkaProcess() throws IOException, InterruptedException {
		List<Integer> processIds = new ArrayList<>();

		Path shell = FileSystems.getDefault().getPath("getIds.sh");
		if (Files.exists(shell)) {
			Files.delete(shell);
		}

		Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxr-x---");
		FileAttribute<Set<PosixFilePermission>> attrs = PosixFilePermissions.asFileAttribute(perms);
		Files.createFile(shell, attrs);

		List<String> lines = Arrays.asList("ps ax | grep -i 'kafka_' | grep java | grep -v grep | awk '{print $1}'");

		Files.write(shell, lines, Charset.forName("UTF-8"));

		ProcessBuilder builder = new ProcessBuilder("./getIds.sh");
		Process p;
		String line;
		p = builder.start();
		p.waitFor();
		BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
		while ((line = br.readLine()) != null) {
			LOG.info("ID = " + line);

			processIds.add(Integer.parseInt(line));
		}
		Files.delete(shell);

		return processIds;
	}

	private List<Integer> getKafkaBrokerIds(List<Integer> preIds) throws IOException, InterruptedException {
		List<Integer> currentIds;
		List<Integer> brokerIds = new ArrayList<>();
		currentIds = getCurrentKafkaProcess();

		for (int i = 0; i < currentIds.size(); i++) {
			LOG.info(currentIds.get(i).toString());
			boolean found = true;
			for (int j = 0; j < preIds.size(); j++) {

				if (preIds.get(j).intValue() == currentIds.get(i).intValue()) {

					found = false;
				}
			}

			if (found) {
				brokerIds.add(currentIds.get(i).intValue());
				LOG.info("Broker Id = {}", currentIds.get(i).intValue());
				debugLogger.info("Broker Id = " + currentIds.get(i).intValue());
			} else {
				debugLogger.info("Background appIds = " + currentIds.get(i).toString());
			}

		}

		return brokerIds;
	}

	private int getOpenPort() {
		LOG.info("Looking for available port to start kafka broker");
		int port = 9092;
		boolean open = true;
		while (true) {
			List<String> command = new ArrayList<String>();
			command.add("netstat");
			command.add("-lntu");
			LOG.info(command.toString());
			ProcessBuilder builder = new ProcessBuilder(command);
			Process p;
			String line;
			try {
				p = builder.start();
				p.waitFor();
				BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ((line = br.readLine()) != null) {
					// LOG.info(line);
					if (line.contains(Integer.toString(port))) {
						open = false;
					}
					;
				}
				if (open) {
					LOG.info("available port = {}", port);
					return port;
				}
				open = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			port++;
		}
	}

	private void kafkaServerStart() {

		List<String> command = new ArrayList<String>();
		command.add(kbInfo.getKafkaBinDir() + "/bin/kafka-server-start.sh");
		command.add(kbInfo.getKafkaBinDir() + "/config/" + MOHA_Properties.KAFKA_SERVER_PROPERTIES);

		LOG.info(command.toString());
		ProcessBuilder builder = new ProcessBuilder(command);
		Process p;
		String line;
		debugLogger.info("ProcessBuilder builder = new ProcessBuilder(command);");
		try {

			p = builder.start();
			debugLogger.info("p = builder.start();");

			Thread.sleep(2000);
			debugLogger.info("p.waitFor()");
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((line = br.readLine()) != null) {
				debugLogger.info(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void kafkaServerStop(List<Integer> appId) throws IOException, InterruptedException {
		Runtime rt = Runtime.getRuntime();
		rt.exec("kill -9 " + appId.get(0).intValue());
	}

	private void kafkaConfig() throws IOException {
		BufferedWriter out;
		BufferedReader br;
		ArrayList<String> lines;

		LOG.info("Configuring server properties --------------------------------------");

		lines = new ArrayList<String>();
		String line = null;

		try {

			File serverProperties = new File(kbInfo.getKafkaBinDir() + "/config/server.properties");
			File brokerProperties = new File(
					kbInfo.getKafkaBinDir() + "/config/" + MOHA_Properties.KAFKA_SERVER_PROPERTIES);
			serverProperties.setWritable(true);
			FileReader fr = new FileReader(serverProperties);
			br = new BufferedReader(fr);
			line = br.readLine();
			while (line != null) {
				if (line.contains("broker.id=0")) {
					line = line.replace("broker.id=0", "broker.id=" + kbInfo.getBrokerId());
					LOG.info("Setting broker id = {}", line);
					debugLogger.info(line);
				}

				if (line.contains("#listeners=PLAINTEXT://:9092")) {
					line = line.replace("#listeners=PLAINTEXT://:9092",
							"listeners=PLAINTEXT://:" + Integer.toString(getOpenPort()));
					LOG.info("Setting port = {}", line);
					debugLogger.info(line);
				}

				if (line.contains("log.dirs=/tmp/kafka-logs")) {
					line = line.replace("log.dirs=/tmp/kafka-logs", "log.dirs=" + MOHA_Properties.KAFKA_LOG_DIR);
					LOG.info("Setting log directory = {}", line);
					debugLogger.info(line);
				}

				if (line.contains("zookeeper.connect=localhost:2181")) {
					line = line.replace("zookeeper.connect=localhost:2181",
							"zookeeper.connect=localhost:2181/" + kbInfo.getConf().getKafkaClusterId());
					LOG.info("Setting Zookeeper directory = {}", line);
					debugLogger.info(line);
				}

				if (line.contains("#delete.topic.enable=true")) {
					line = line.replace("#delete.topic.enable=true", "delete.topic.enable=true");
					LOG.info("Enable delete topic immediately");
					debugLogger.info(line);
				}

				lines.add(line);
				line = br.readLine();
			}

			fr.close();
			br.close();
			debugLogger.info("start writing broker properties");
			FileWriter fw = new FileWriter(brokerProperties);
			out = new BufferedWriter(fw);
			for (String s : lines)
				out.write(s + '\n');

			out.flush();
			out.close();
			debugLogger.info("end writing broker properties");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	protected class ThreadKafkaStart implements Runnable {

		public ThreadKafkaStart() {
			// TODO Auto-generated constructor stub
			LOG.info("ThreadKafkaStart");
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub

			try {
				kafkaConfig();
				kafkaServerStart();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}

	}

}
