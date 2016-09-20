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

	public Connection getConnection() {

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

	public void runCommand(String sql) {
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

	public void executorInsert(MOHA_ExecutorInfo eInfo) {
		// TODO Auto-generated method stub
		String sql = "insert into " + MOHA_Properties.ExecutorDb
				+ "(appId, executorId, containerId, hostname, launchedTime,  numExecutedTasks, runningTime, launchedTimeMiniSeconds, firstMessageTime, pollingtime,endingTime) values (\""
				+ eInfo.getAppId() + "\"" + ",\"" + eInfo.getExecutorId() + "\"" + ",\"" + eInfo.getContainerId() + "\""
				+ ",\"" + eInfo.getHostname() + "\"" + ",\"" + convertLongToDate(eInfo.getLaunchedTime()) + "\"" + ","
				+ eInfo.getNumExecutedTasks() + "," + eInfo.getRunningTime() + "," + eInfo.getLaunchedTime()+ "," + eInfo.getFirstMessageTime()+ "," + eInfo.getPollingTime() + "," + eInfo.getEndingTime() +")";

		runCommand(sql);
	}

	public void appInfoInsert(MOHA_AppInfo appInfo) {
		String sql = "insert into " + MOHA_Properties.AppDb
				+ "(appId, executorMemory, numExecutors, numPartitions, startingTime, initTime,makespan, numCommands, command) values (\""
				+ appInfo.getAppId() + "\"," + appInfo.getExecutorMemory() + "," + appInfo.getNumExecutors() + ","
				+ appInfo.getNumPartitions() + "," + "\" " + convertLongToDate(appInfo.getStartingTime()) + "\","+ appInfo.getInitTime() + ","
				+ appInfo.getMakespan() + "," + appInfo.getNumCommands() + ",\"" + appInfo.getCommand() + "\")";

		runCommand(sql);
		sql = "update " +  MOHA_Properties.ExecutorDb + " set " + " allocationTime = " + appInfo.getAllocationTime() + " where appId = \""  + appInfo.getAppId() +"\"";
		runCommand(sql);
	}

	public String convertLongToDate(long dateMilisecs) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

		GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("US/Central"));
		calendar.setTimeInMillis(dateMilisecs);

		String dateFormat = sdf.format(calendar.getTime());

		return dateFormat;
	}

	public long convertDateToLong(String dateTime) {
		long timeMillis = 0;

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
		if (dateTime != null) {
			try {
				Date timeDate = sdf.parse(dateTime);
				if (timeDate != null) {
					timeMillis = timeDate.getTime();
				}

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				System.out.println("Date time: " + dateTime);
				e.printStackTrace();
			}
		}
		return timeMillis;
	}

	public static void main(String[] args) {
		MOHA_AppInfo appInfo = new MOHA_AppInfo();
		MOHA_ExecutorInfo info = new MOHA_ExecutorInfo();
		MOHA_Database data = new MOHA_Database();
		data.appInfoInsert(appInfo);
		data.executorInsert(info);
	}
}
