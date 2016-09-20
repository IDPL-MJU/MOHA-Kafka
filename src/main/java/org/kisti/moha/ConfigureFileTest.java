package org.kisti.moha;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigureFileTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties prop = new Properties();
		/* Loading MOHA.Conf File */
		try {
			prop.load(new FileInputStream("conf/MOHA.conf"));
			String kafka_libs = prop.getProperty("MOHA.dependencies.kafka.libs");
			System.out.println(kafka_libs);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * Reading in the URL of the DBManager in the configuration file
		 * e.g., DBManager.Address=http://150.183.158.172:9000/Database
		 */
		
	}

}
