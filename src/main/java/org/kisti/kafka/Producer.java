package org.kisti.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/* This looks as test codes */
public class Producer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<String> command = new ArrayList<String>();
		command.add("sleep");
		command.add("0.05");
		
		for (int i = 0; i < 1000; i++) {
			try {
				ProcessBuilder builder = new ProcessBuilder(command);
				Process process;
				process = builder.start();
				process.waitFor();
				/*
				 * BufferedReader br = new BufferedReader(new
				 * InputStreamReader(p.getInputStream())); while ((line =
				 * br.readLine()) != null) { outQueue.push( "Task Executor (" +
				 * id + ") " + "from partition  " + record.partition() + " : " +
				 * line); }
				 */
				
				
				System.out.println("i = " + i);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		String s = "I want to        walk dsfds 3243 ; ; /.,[]my dog";

		 String[] arr = s.split(" ");    

		 for ( String ss : arr) {

		       System.out.println(ss);
		  }

	}

}
