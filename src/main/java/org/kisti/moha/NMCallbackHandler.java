package org.kisti.moha;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

	private static final Logger LOG = LoggerFactory.getLogger(NMCallbackHandler.class);

	private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
	private final MOHA_Manager mohaManager;
	MOHA_Queue log_queue;

	public NMCallbackHandler(MOHA_Manager applicationMaster) {
		this.mohaManager = applicationMaster;
		log_queue = new MOHA_Queue("test");
		log_queue.register();
		
		LOG.info(log_queue.push("nmClient.start(); ..."));
	}

	public void addContainer(ContainerId containerId, Container container) {
		containers.putIfAbsent(containerId, container);
		LOG.info(log_queue.push(" addContainer " + containerId ));
	}

	@Override
	public void onContainerStopped(ContainerId containerId) {
		LOG.debug("Succeeded to stop Container {}", containerId);
		LOG.info(log_queue.push(" onContainerStopped " + containerId ));
		containers.remove(containerId);
	}

	@Override
	public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
		LOG.debug("Container Status: id = {}, status = {}", containerId, containerStatus);
		LOG.info(log_queue.push(" onContainerStatusReceived " + containerId ));
	}

	@Override
	public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
		LOG.debug("Succeeded to start Container {}", containerId);
		
		LOG.info(log_queue.push(" onContainerStarted " + containerId ));
		Container container = containers.get(containerId);
		if (container != null) {
			LOG.info(log_queue.push(" onContainerStarted" + container.toString()));
			mohaManager.nmClient.getContainerStatusAsync(containerId, container.getNodeId());
		}
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		LOG.info(log_queue.push(" onStartContainerError " + containerId ));
		LOG.error("Failed to start Container {}", containerId);
		containers.remove(containerId);
		mohaManager.numCompletedContainers.incrementAndGet();
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
		LOG.info(log_queue.push(" onGetContainerStatusError " + containerId ));
		LOG.error("Failed to query the status of Container {}", containerId);
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable t) {
		LOG.info(log_queue.push(" onStopContainerError " + containerId ));
		LOG.error("Failed to stop Container {}", containerId);
		containers.remove(containerId);
	}
}