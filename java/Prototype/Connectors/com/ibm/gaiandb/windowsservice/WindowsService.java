/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.windowsservice;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

public class WindowsService {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";	

	private static boolean run = true;
	private static Properties prop = new Properties();

	public static void main(String[] args) {
		if ((args.length == 0) || (!args[0].equals("start")) || (!args[0].equals("stop"))) {
			System.out
					.println("You must pass the string \"start\" or \"stop\" as the first argument if starting via the main method");
		}

		String[] newArgs = new String[args.length - 1];
		System.arraycopy(args, 1, newArgs, 0, newArgs.length);

		if (args[0].equals("stop")) {
			stop(newArgs);
		} else {
			start(newArgs);
		}
	}

	public static void start(String[] args) {

		try {
			readProperties(args[0]);
		} catch (FileNotFoundException e) {
			System.out.println("ERROR: could not find properties file.");
			System.out
					.println("You must specify a properties file as the first parameter.  The properties file should be located in the same directory as the jar or else you must set up the classpath accordingly.");
			e.printStackTrace();
			return;
		} catch (IOException e) {
			System.out.println("ERROR: unable to read properties file.");
			System.out
					.println("You must specify a properties file as the first parameter.  The properties file should be located in the same directory as the jar or else you must set up the classpath accordingly.");
			e.printStackTrace();
			return;
		} 

		Process p = null;
		String cmd = prop.getProperty("command");
		String[] env = getEnv();
		File dir = new File(prop.getProperty("workingdirectory", "."));
		
		if (cmd == null || cmd.isEmpty()) {
			System.out.println("The \"command=\" property MUST be specified in the properties file");
			return;
		}

		System.out.println("Executing command: " + cmd);
		System.out.println("Working directory: " + dir.toString());

		try {
			p = Runtime.getRuntime().exec(cmd, env, dir);
		} catch (IOException e) {
			System.out.println("Exception raised executing the command, see standard error output for stack trace.");
			e.printStackTrace();
			return;
		}

		System.out.println("Command executed, command output follows..." + System.getProperty("line.separator"));

		String line;
		InputStreamReader is = new InputStreamReader(p.getInputStream());
		BufferedReader in = new BufferedReader(is);

		try {
			while (run) {
				// read from the buffer until its empty.
				while (in.ready()) {
					line = in.readLine();
					if (line != null) {
						System.out.println(line);
					} else {
						System.out.println("Finished reading output from command");
						break;
					}
				}
				try {
				Thread.sleep(100); //sleep for tenth second before trying to read again.
				} catch (InterruptedException ie){
					// carry on round the loop. we will exit if the work is complete.
				}
			}
		} catch (IOException e) {
			System.out.println("Exception raised reading output from command, see standard error output for stack trace.");
			e.printStackTrace();
		}

		System.out.println(System.getProperty("line.separator") + "...end of command output, stopping command.");
		p.destroy();
		System.out.println("Exit code: " + p.exitValue());
	}

	public static void stop(String[] args) {
		run = false;
	}

	private static String[] getEnv() {
		Map<String, String> envMap = System.getenv();
		String[] env = new String[envMap.size()];
		int i = 0;
		for (String envName : envMap.keySet()) {
			env[i] = envName + "=" + envMap.get(envName);
			i++;
		}
		return env;
	}

	private static void readProperties(String filename) throws IOException {
		System.out.println("Props file: " + filename);
		InputStream input = null;
		input = WindowsService.class.getClassLoader().getResourceAsStream(filename);
		if (input == null) {
			throw new FileNotFoundException();
		}
		prop.load(input);
		input.close();
	}
}
