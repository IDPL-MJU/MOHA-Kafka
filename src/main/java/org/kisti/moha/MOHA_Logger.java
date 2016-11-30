package org.kisti.moha;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MOHA_Logger {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_Logger.class);
	MOHA_Queue debugQueue;
	
	private boolean isKafkaAvailable;
	private boolean isLogEnable;
	
	public MOHA_Logger(boolean isLogEnable, String queueName) {
		
		this.isKafkaAvailable = new MOHA_Zookeeper(null).isKafkaDebugServiceAvailable();
		this.isLogEnable = isLogEnable;
		
		if (isLogEnable&&isKafkaAvailable) {
			debugQueue = new MOHA_Queue(queueName);
			debugQueue.register();
		}
		LOG.info(this.toString());

	}

	@Override
	public String toString() {
		return "MOHA_Logger [debugQueue=" + debugQueue + ", isKafkaAvailable=" + isKafkaAvailable + ", isLogEnable="
				+ isLogEnable + "]";
	}

	public String info(String info) {
		if (isLogEnable&&isKafkaAvailable) {
			debugQueue.push(info);
		}
		return info;
	}

}
