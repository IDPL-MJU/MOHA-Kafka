package org.kisti.moha;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MOHA_Database {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_TaskExecutor.class);

	private Connection getConnection() {

		String URL = MOHA_Properties.Db;
		String username = "moha_user";
		String password = "password";

		Connection connection = null;
		LOG.info("getConnection");
		try {
			LOG.info("Start getting connection");
			Class.forName("com.mysql.jdbc.Driver");
			DriverManager.setLoginTimeout(10);
			connection = DriverManager.getConnection(URL, username, password);
			LOG.info("Connection = {}", connection.toString());
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;

	}

	private void runCommand(String sql) {
		Connection connection = null;
		PreparedStatement preStmt = null;
		LOG.info("sql command : {}", sql);
		connection = getConnection();
		try {
			preStmt = connection.prepareStatement(sql);
			preStmt.execute();
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void insertExecutorInfoToDatabase(MOHA_ExecutorInfo eInfo) {
		if (MOHA_Properties.DEBUG_MYSQL) {
			String sql = "insert into " + MOHA_Properties.ExecutorDb
					+ "(appId, executorId, containerId, hostname, launchedTime,  numExecutedTasks, runningTime, launchedTimeMiniSeconds, firstMessageTime, pollingtime,endingTime) values (\""
					+ eInfo.getAppId() + "\"" + ",\"" + eInfo.getExecutorId() + "\"" + ",\"" + eInfo.getContainerId()
					+ "\"" + ",\"" + eInfo.getHostname() + "\"" + ",\"" + MOHA_Common.convertLongToDate(eInfo.getLaunchedTime())
					+ "\"" + "," + eInfo.getNumExecutedTasks() + "," + eInfo.getRunningTime() + ","
					+ eInfo.getLaunchedTime() + "," + eInfo.getFirstMessageTime() + "," + eInfo.getPollingTime() + ","
					+ eInfo.getEndingTime() + ")";

			runCommand(sql);
		} else {
			LOG.info("----------------");
		}

	}

	public void insertAppInfoToDababase(MOHA_Info appInfo) {

		if (MOHA_Properties.DEBUG_MYSQL) {
			String sql = "insert into " + MOHA_Properties.AppDb
					+ "(appId, executorMemory, numExecutors, numPartitions, startingTime, initTime,makespan, numCommands, command) values (\""
					+ appInfo.getAppId() + "\"," + appInfo.getExecutorMemory() + "," + appInfo.getNumExecutors() + ","
					+ appInfo.getNumPartitions() + "," + "\" " + MOHA_Common.convertLongToDate(appInfo.getStartingTime()) + "\","
					+ appInfo.getInitTime() + "," + appInfo.getMakespan() + "," + appInfo.getNumCommands() + ",\""
					+ appInfo.getCommand() + "\")";

			runCommand(sql);
			sql = "update " + MOHA_Properties.ExecutorDb + " set " + " allocationTime = " + appInfo.getAllocationTime()
					+ " where appId = \"" + appInfo.getAppId() + "\"";
			runCommand(sql);
		} else {
			LOG.info("----------------");
		}
	}



	public static void main(String[] args) {
		MOHA_Info appInfo = new MOHA_Info();
		MOHA_ExecutorInfo info = new MOHA_ExecutorInfo();
		MOHA_Database data = new MOHA_Database();
		data.insertAppInfoToDababase(appInfo);
		data.insertExecutorInfoToDatabase(info);
	}
}
