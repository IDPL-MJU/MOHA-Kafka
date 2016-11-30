package org.kisti.moha;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MOHA_Zookeeper {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_Zookeeper.class);
	private static String zookeeperDir;
	private static ZooKeeper zk;

	private static String getZookeeperDir() {
		return zookeeperDir;
	}

	public MOHA_Zookeeper(String zookeeperDir) {
		setZookeeperDir(zookeeperDir);

		final CountDownLatch connSignal = new CountDownLatch(0);
		LOG.info("Connecting to the zookeeper server");
		try {
			zk = new ZooKeeper("localhost", 30000, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					// TODO Auto-generated method stub
					if (event.getState() == KeeperState.SyncConnected) {
						connSignal.countDown();
					}
				}
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			connSignal.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void setZookeeperDir(String zookeeperDir) {
		MOHA_Zookeeper.zookeeperDir = zookeeperDir;
	}

	public boolean exist() throws KeeperException, InterruptedException {
		String rootDirs = "/" + getZookeeperDir();
		if (zk != null) {
			if (zk.exists(rootDirs, false) != null) {
				return true;
			}
		}
		return false;
	}

	public void setRequests(Boolean stop) throws IOException, InterruptedException, KeeperException {

		String rootDirs = "/" + getZookeeperDir();
		String requestDirs = rootDirs + "/rq";

		if (zk != null) {
			try {
				Stat root = zk.exists(rootDirs, false);
				if (root == null) {
					LOG.info("broker is not running yet");
					return;
				} else {
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
				}

			} catch (KeeperException e) {
				System.out.println("Keeper exception when instantiating queue: " + e.toString());
			} catch (InterruptedException e) {
				System.out.println("Interrupted exception");
			}
		}

	}

	public String getBootstrapServers() throws IOException, InterruptedException, KeeperException {

		// Show broker server information
		String zookeeperConnect;
		StringBuilder command = new StringBuilder();
		String idsDirs = "/" + getZookeeperDir() + "/brokers/ids";

		// command.append("\"");
		List<String> ids = zk.getChildren(idsDirs, false);
		LOG.info("ids = {}", ids.toString());
		for (String id : ids) {
			String brokerInfo = new String(zk.getData(idsDirs + "/" + id, false, null));
			LOG.info("server = {}",
					brokerInfo.substring(brokerInfo.lastIndexOf("[") + 14, brokerInfo.lastIndexOf("]") - 1));
			command.append(brokerInfo.substring(brokerInfo.lastIndexOf("[") + 14, brokerInfo.lastIndexOf("]") - 1))
					.append(",");
		}
		command.deleteCharAt(command.length() - 1);
		// command.append("\"");
		zookeeperConnect = command.toString();

		return zookeeperConnect;
	}

	public boolean isKafkaDebugServiceAvailable() {
		if (zk != null) {
			String idsDirs = "/brokers/ids";

			try {
				List<String> ids = zk.getChildren(idsDirs, false);
				if(ids.size() > 2){
					return true;
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return false;
	}

	public boolean checkRequests() throws IOException, InterruptedException, KeeperException {

		LOG.info("Checking request from MOHA manager");
		String rootDirs = "/" + getZookeeperDir();
		String requestDirs = rootDirs + "/rq";
		if (zk != null) {
			if (zk.exists(rootDirs, false) != null) {
				if (zk.exists(requestDirs, false) != null) {
					String rqInfo = new String(zk.getData(requestDirs, false, null));

					LOG.info("rqInfo ------------------------- = {}", rqInfo);

					return Boolean.parseBoolean(rqInfo);

				}
			}
		}

		return true;

	}

	public void close() {
		try {
			zk.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void delete() {
		String rootDirs = "/" + getZookeeperDir();
		try {
			zk.delete(rootDirs, zk.exists(rootDirs, true).getVersion());
		} catch (InterruptedException | KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
