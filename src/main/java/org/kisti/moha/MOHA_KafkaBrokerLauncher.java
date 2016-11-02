package org.kisti.moha;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class MOHA_KafkaBrokerLauncher {
	private final Logger LOG = LoggerFactory.getLogger(MOHA_KafkaBrokerLauncher.class);
	private MOHA_KafkaBrokerInfo kbInfo;
	private static MOHA_Queue outQueue;

	public MOHA_KafkaBrokerLauncher(String[] args) {
		LOG.info("Container just started on {}" + NetUtils.getHostname());

		String outputQueueName = "test";
		outQueue = new MOHA_Queue(outputQueueName);
		outQueue.register();
		LOG.info(outQueue.push("Start MOHA_KafkaBrokerLauncher constructor on " + NetUtils.getHostname()));

		// System.setProperty("java.library.path", "kafka_2.11-0.10.0.0/libs");
		/*Field fieldSysPath;
		try {
			fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
			fieldSysPath.setAccessible(true);
			try {
				fieldSysPath.set(null, null);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

		LOG.info(outQueue.push("Init Kafka broker manager =" + args.toString()));
		
		kbInfo = new MOHA_KafkaBrokerInfo();
		kbInfo.setAppId(args[0]);
		kbInfo.setContainerId(args[1]);
		kbInfo.setBrokerId(args[2]);
		kbInfo.setKafkaLibsDir(args[3]);
		kbInfo.setZookeeperConnect("localhost:2181/" + kbInfo.getAppId());
		
		LOG.info(outQueue.push("getAppId =" + kbInfo.getAppId()));
		LOG.info(outQueue.push("getContainerId =" + kbInfo.getContainerId()));
		LOG.info(outQueue.push("getBrokerId =" + kbInfo.getBrokerId()));
		LOG.info(outQueue.push("getKafkaLibVersion =" + kbInfo.getKafkaLibsDir()));
		LOG.info(outQueue.push("getZookeeperConnect =" + kbInfo.getZookeeperConnect()));

		File dir = new File(".");
		File[] filesList = dir.listFiles();
		for (File file : filesList) {
		    if (file.isFile()) {
		    	outQueue.push(file.getName());
		    }
		}
		
	}

	public void run() throws IOException, InterruptedException, KeeperException {
		LOG.info("It's about to start Kafka brokers");

		ThreadKafkaStart kafkaStart = new ThreadKafkaStart();
		Thread startThread = new Thread(kafkaStart);
		// MOHA_Queue queue = new MOHA_Queue(kbInfo.getZookeeperConnect(),
		// getBootstrapServers(), "test");
		// queue.register();
		startThread.start();outQueue.push("startThread.start()");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*setRequests(false);
		while (!checkRequests() && (getBootstrapServers().length() > 3)) {
			LOG.info("Kafka server is running = {}", getBootstrapServers());
			// queue.push("Kafka server is running");
			try {
				Thread.sleep(2000);
				setRequests(true);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
		ThreadKafkaStop kafkaStop = new ThreadKafkaStop();
		Thread stopThread = new Thread(kafkaStop);
		stopThread.start();outQueue.push("stopThread.start()");
		
		LOG.info("Kafka broker is stopped");
		return;
	};

	public static void main(String[] args) {

		MOHA_KafkaBrokerLauncher brokerManager = new MOHA_KafkaBrokerLauncher(args);

		try {
			brokerManager.run();
		} catch (IOException | InterruptedException | KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return;
	}

	public int getOpenPort() {
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

	public boolean checkRequests() throws IOException, InterruptedException, KeeperException {

		LOG.info("Checking request from MOHA manager");
		String requestDirs = "/" + kbInfo.getAppId() + "/rq";

		final CountDownLatch connSignal = new CountDownLatch(0);
		ZooKeeper zk = new ZooKeeper("localhost", 30000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				if (event.getState() == KeeperState.SyncConnected) {
					connSignal.countDown();
				}
			}
		});
		connSignal.await();
		String rqInfo = new String(zk.getData(requestDirs, false, null));

		LOG.info("rqInfo ------------------------- = {}", rqInfo);
		if (Boolean.parseBoolean(rqInfo)) {
			zk.delete(requestDirs, zk.exists(requestDirs, true).getVersion());
		}
		zk.close();

		return Boolean.parseBoolean(rqInfo);
	}

	public void setRequests(Boolean stop) throws IOException, InterruptedException, KeeperException {

		String requestDirs = "/" + kbInfo.getAppId() + "/rq";

		final CountDownLatch connSignal = new CountDownLatch(0);
		LOG.info("Connecting to the zookeeper server");
		ZooKeeper zk = new ZooKeeper("localhost", 30000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				if (event.getState() == KeeperState.SyncConnected) {
					connSignal.countDown();
				}
			}
		});
		connSignal.await();

		if (zk != null) {
			try {
				Stat s = zk.exists(requestDirs, false);
				if (s == null) {
					LOG.info("Creating a znode for request");
					zk.create(requestDirs, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

				}
				LOG.info("Making the request");
				if (stop) {
					zk.setData(requestDirs, "true".getBytes(), -1);
				} else {
					zk.setData(requestDirs, "false".getBytes(), -1);
				}
			} catch (KeeperException e) {
				System.out.println("Keeper exception when instantiating queue: " + e.toString());
			} catch (InterruptedException e) {
				System.out.println("Interrupted exception");
			}
		}

		zk.close();
	}

	public String getBootstrapServers() throws IOException, InterruptedException, KeeperException {

		// Show broker server information
		String zookeeperConnect;
		StringBuilder command = new StringBuilder();
		String idsDirs = "/" + kbInfo.getAppId() + "/brokers/ids";
		final CountDownLatch connSignal = new CountDownLatch(0);
		ZooKeeper zk = new ZooKeeper("localhost", 30000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				if (event.getState() == KeeperState.SyncConnected) {
					connSignal.countDown();
				}
			}
		});
		connSignal.await();

		command.append("\"");
		List<String> ids = zk.getChildren(idsDirs, false);
		LOG.info("ids = {}", ids.toString());
		for (String id : ids) {
			String brokerInfo = new String(zk.getData(idsDirs + "/" + id, false, null));
			// LOG.info(id + ": " + brokerInfo);
			LOG.info("server = {}",
					brokerInfo.substring(brokerInfo.lastIndexOf("[") + 14, brokerInfo.lastIndexOf("]") - 1));
			command.append(brokerInfo.substring(brokerInfo.lastIndexOf("[") + 14, brokerInfo.lastIndexOf("]") - 1))
					.append(",");
		}
		command.deleteCharAt(command.length() - 1);
		command.append("\"");
		zookeeperConnect = command.toString();
		zk.close();
		return zookeeperConnect;
	}

	public void kafkaServerStart() {

		List<String> command = new ArrayList<String>();
		command.add(kbInfo.getKafkaLibsDir() + "/bin/kafka-server-start.sh");
		command.add(kbInfo.getKafkaLibsDir() + "/config/" + MOHA_Properties.serverPros);

		LOG.info(command.toString());
		ProcessBuilder builder = new ProcessBuilder(command);
		Process p;
		String line;outQueue.push("ProcessBuilder builder = new ProcessBuilder(command);");
		try {outQueue.push("try {");
			p = builder.start();outQueue.push("p = builder.start();");
			p.waitFor();outQueue.push("p.waitFor()");
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((line = br.readLine()) != null) {
				LOG.info(line);outQueue.push(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void kafkaServerStop() {
		String command = kbInfo.getKafkaLibsDir() + "/bin/kafka-server-stop.sh";
		LOG.info(command);
		ProcessBuilder builder = new ProcessBuilder(command);
		Process p;
		String line;outQueue.push(command);
		try {
			p = builder.start();outQueue.push("p = builder.start()");
			p.waitFor();outQueue.push("p.waitFor()");
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((line = br.readLine()) != null) {
				LOG.info(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void kafkaConfig() throws IOException {
		BufferedWriter out;
		BufferedReader br;
		ArrayList<String> lines;

		LOG.info("Configuring server properties --------------------------------------");

		lines = new ArrayList<String>();
		String line = null;

		try {

			File serverProperties = new File(kbInfo.getKafkaLibsDir() + "/config/server.properties");
			File brokerProperties = new File(kbInfo.getKafkaLibsDir() + "/config/" + MOHA_Properties.serverPros);
			serverProperties.setWritable(true);
			FileReader fr = new FileReader(serverProperties);
			br = new BufferedReader(fr);
			line = br.readLine();
			while (line != null) {
				if (line.contains("broker.id=0")) {
					line = line.replace("broker.id=0", "broker.id=" + kbInfo.getBrokerId());
					LOG.info("Setting broker id = {}", line);outQueue.push(line);
				}

				if (line.contains("#listeners=PLAINTEXT://:9092")) {
					line = line.replace("#listeners=PLAINTEXT://:9092",
							"listeners=PLAINTEXT://:" + Integer.toString(getOpenPort()));
					LOG.info("Setting port = {}", line);outQueue.push(line);
				}

				if (line.contains("log.dirs=/tmp/kafka-logs")) {
					line = line.replace("log.dirs=/tmp/kafka-logs", "log.dirs=" + MOHA_Properties.kafkaLogDir);
					LOG.info("Setting log directory = {}", line);outQueue.push(line);
				}

				if (line.contains("zookeeper.connect=localhost:2181")) {
					line = line.replace("zookeeper.connect=localhost:2181",
							"zookeeper.connect=localhost:2181/" + kbInfo.getAppId());
					LOG.info("Setting Zookeeper directory = {}", line);outQueue.push(line);
				}

				lines.add(line);
				line = br.readLine();
			}

			fr.close();
			br.close();
			outQueue.push("start writing broker properties");
			FileWriter fw = new FileWriter(brokerProperties);
			out = new BufferedWriter(fw);
			for (String s : lines)
				out.write(s + '\n');

			out.flush();
			out.close();
			outQueue.push("end writing broker properties");
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

	protected class ThreadKafkaStop implements Runnable {
		@Override
		public void run() { // TODO Auto-generated method stub
			kafkaServerStop();
			return;
		}

	}

}
