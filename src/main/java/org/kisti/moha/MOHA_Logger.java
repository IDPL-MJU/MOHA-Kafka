package org.kisti.moha;

public class MOHA_Logger {
	MOHA_Queue debugQueue;
	private String queueName = "test";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public MOHA_Logger() {
		if (MOHA_Properties.DEBUG_KAFKALOG) {
			debugQueue = new MOHA_Queue(queueName);
			debugQueue.register();

		}

	}

	public String info(String info) {
		if (MOHA_Properties.DEBUG_KAFKALOG) {
			debugQueue.push(info);
		}
		return info;
	}

}
