/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class BootstrapService {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";
	
	// get GAIAN_HOME, JAVA_HOME or JRE_HOME from system environment
	// TODO: improvement: see if dependency on these paths can be reduced
	private static String gaianHome = System.getenv("GAIAN_HOME");
	private static String javaHome = System.getenv("JAVA_HOME");
	private static String jreHome = System.getenv("JRE_HOME");
	private static String gaianLibDir = gaianHome + "/lib";
	
	private static String gaianClasspath = null;
	private static String processId = null;
	
	/*
	 * Build classpath by adding all files from GAIAN_HOME/lib
	 * Note: it means ALL dependencies for poi, 3rd party DBs need to be in GAIAN_HOME/lib
	 * TODO: see if this can be changed to use multiple directories
	 */
	private static void addFilesInDirectoryToClasspath(File dir, StringBuffer classpath) {
	  File[] files = dir.listFiles();
	    if (files != null) {
	      for (File f : files) {
	         if (f.isDirectory()) {
	        	 addFilesInDirectoryToClasspath(f, classpath);
		 }
		 else {
		    //System.out.println(f.getName());
		    // add to classpath
		    classpath.append(f.getAbsolutePath());
		    classpath.append(isWindows() ? ";" : ":");
		}
	     }
	  }
	}
	
	/*
	 * Determine OS. Windows or not.
	 */
	public static boolean isWindows() {
		boolean isWindows = false;

		String osName = System.getProperty("os.name");

		if (osName != null && Pattern.compile(Pattern.quote("win"), Pattern.CASE_INSENSITIVE).matcher(osName).find()) {
			isWindows = true;
		}

		return isWindows;
	}

	/*
	 * Start has been called on the service, let's start the node with arguments collected from script
	 */
	public static void start(String args[]) throws Exception {
		if (gaianHome == null && (javaHome == null || jreHome == null)) {
			throw new Exception("GAIAN_HOME and JAVA_HOME (or JRE_HOME) are not set. Please set GAIAN_HOME and JAVA_HOME (or JRE_HOME) environment variables");
		}
		
		/* Uncomment to debug incoming args
		for (String arg: args) {
			System.out.println(arg);
		}*/
		
		// build classpath
		StringBuffer classpath = new StringBuffer();
		addFilesInDirectoryToClasspath(new File(gaianLibDir), classpath);	
		gaianClasspath = classpath.toString();
		
		String usingJava = null;
		
		if (jreHome != null && !jreHome.equals("")) {
			System.out.println(jreHome);
			if (jreHome.endsWith("jre"))
				usingJava = jreHome + "/bin/java";
			else 
				usingJava = jreHome + "/jre/bin/java";				
		} else if (javaHome != null && !javaHome.equals("")) {
			System.out.println(javaHome);
			usingJava = javaHome + "/bin/java";
		} else {
			System.out.println("using java.exe in the PATH");
			usingJava = "java";
		}
		
		// build command
		ArrayList<String> command = new ArrayList<String>();
		command.add(usingJava);
		command.add("-Xmx256m");
		command.add("-cp");
		command.add(gaianClasspath);

		// find the arguments passed
		ArrayList<String> gaianArgs = null;
		String jvmArgs = null;
		
		if (args.length > 1) { // first argument is always 'start'
			for (int i = 1; args.length > i; i++) {
				String arg = args[i];
				System.out.println(arg);
				
				if (arg.startsWith("-jvmargs")) {
					arg = args[i+1];
					jvmArgs = arg.trim(); System.out.println("adding JVM arguments: " + jvmArgs);
				} else if (arg.startsWith("-gdbargs")){
					System.out.println("Adding extra arguments: " + arg);
					gaianArgs = new ArrayList<String>();
					boolean hasMoreArgs = true;
					arg = args[i+1];
					System.out.println(arg);
					// need to tokenize the gaian args

					while (hasMoreArgs) {
						if (arg.length() > 0) {
							int idx = arg.indexOf(" ");
							if (idx > 0) {
								System.out.println(arg.substring(0, idx).trim());
								gaianArgs.add(arg.substring(0, idx).trim());
							} else {
								hasMoreArgs = false;
							}

							if (idx > 0)
								arg = arg.substring(idx).trim();
							else
								hasMoreArgs = false;
							
							System.out.println(arg);
							
							idx = arg.indexOf(" -");
							if (idx > 0) {
								System.out.println(arg.substring(0, idx).trim());
								gaianArgs.add(arg.substring(0, idx).trim());							
							} else {
								hasMoreArgs = false;
							}
							
							System.out.println(arg.length());
							System.out.println(idx);
							
							if (idx > 0)
								arg = arg.substring(idx).trim();
							else {
								if (arg.length() > 0)
									gaianArgs.add(arg.trim());
								hasMoreArgs = false;
							}
								
						} else
							hasMoreArgs = false;
					}
					
					//gaianArgs = arg.split("\\s[-]\\w+\\s");
				} else {
					// do nothing for now, what other args could we get ?
				}
			}	
		}

		// here put the -D stuff
		if (jvmArgs != null)
			command.add(jvmArgs);
		
		command.add("com.ibm.gaiandb.GaianNode");
		if (gaianArgs != null) {
			for (String arg: gaianArgs) {
				command.add(arg);
			}
		}
		
		ProcessBuilder pb = new ProcessBuilder(command);
		File wDF = new File(gaianHome);
		wDF.mkdir();
		pb.directory(wDF);
		
		System.out.println("  Launching Gaian Node using command: " + pb.command());
		
		Process process = pb.start();

		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line;

		while ((line = br.readLine()) != null) {

			System.out.println("\t" + line);

			if (line.contains("PROCESS ID:")) {
				String[] split = line.split("\t");
				processId = split[split.length - 1];
				
				if (processId == null) {
					throw new Exception("Unable to start node. Please check the log.");
				}
			} 
		}	
		
		isr.close();
		is.close();
	}
	
	/*
	 * Stop has been called from service, let's stop this node by using the processId we saved
	 */
	public static void stop(String args[]) throws Exception {
		if (gaianHome == null && javaHome == null) {
			throw new Exception("GAIAN_HOME and JAVA_HOME (or JRE_HOME) are not set. Please set GAIAN_HOME and JAVA_HOME (or JRE_HOME) environment variables");
		}
		
		for (String arg : args) {
			System.out.println(arg);
		}
		
		if (gaianClasspath == null) {
			StringBuffer classpath = new StringBuffer();		
			addFilesInDirectoryToClasspath(new File(gaianLibDir), classpath);
			gaianClasspath = classpath.toString();
		}

		System.out.print("  Attempting to kill node with process id: '"+ processId + "' ... ");

		/*
		 * Execute the appropriate command to kill the node
		 */
		if (isWindows()) {
			Process proc = new ProcessBuilder("taskkill", "/f", "/pid", processId).start();

			StreamSwallower errorSwallower = new StreamSwallower(proc.getErrorStream());
			StreamSwallower outputSwallower = new StreamSwallower(proc.getInputStream());

			errorSwallower.start();
			outputSwallower.start();
		}
	}

	public static class StreamSwallower extends Thread {
		InputStream is;
		BufferedReader br;

		public StreamSwallower(InputStream is) {
			this.is = is;
		}

		public StreamSwallower(BufferedReader br) {
			this.br = br;
		}

		public void run() {
			if (br == null) {
				InputStreamReader isr = new InputStreamReader(is);
				br = new BufferedReader(isr);
			}
			try {
				while (br.readLine() != null) {
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/*
	 * Called by the gaiandb.exe with the arguments collected by the service.bat script
	 */
	public static void main(String args[]) {
		
		if (args.length > 0 && args[0].equals("stop")) {
			try {
				BootstrapService.stop(args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			try {
				BootstrapService.start(args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
