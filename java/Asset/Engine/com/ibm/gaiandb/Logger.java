/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.File;
import java.io.PrintStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author DavidVyvyan
 */
public class Logger {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "Logger", 20 );
	
	public static final String[] POSSIBLE_LEVELS = { "NONE", "LESS", "MORE", "ALL" };
	
	public static final int LOG_NONE = 0;
	public static final int LOG_LESS = 1;
	public static final int LOG_MORE = 2;
	public static final int LOG_ALL = 3;
	
	public static final String LOG_EXCLUDE = "LOG_EXCLUDE:";
	
	public static final int LOG_DEFAULT = LOG_NONE;
	public static int logLevel = LOG_DEFAULT;
	
	private static final String WARNING_PREFIX = "WARNING:\n\n\t********** ";
	private static final String IMPORTANT_PREFIX = "******* ";
	
	public static final String HORIZONTAL_RULE = "\n========================================================" +
	"================================================================================================================\n";
	
	private final String prefix;
	private static boolean isLogTimeStamps = true;
	
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public static final String UNKNOWN_ERROR = "UNKNOWN_ERROR";
	public static final String UNKNOWN_WARNING = "UNKNOWN_WARNING";
	
//	private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyMMdd HH:mm:ss.SSS");
	
	private static PrintStream printStream = System.out;
	
//	private static ZKMStackTraceTranslate stackTraceTranslate;
//
//	static {
////	   dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
////	   dateFormat.setTimeZone(TimeZone.getDefault());
//	   // Get change log file name & classpath to obfuscated bytecode & supporting classes from properties file.
////	   ResourceBundle resourceBundle = ResourceBundle.getBundle("examples.log4j.ZKMSimpleFormatter");      
//	   stackTraceTranslate = new ZKMStackTraceTranslate("ChangeLog.txt");
//	}
	
	public Logger() {
		prefix = "";
	}
	
	public Logger( String kname, int tabchars ) {
		
//		String kname = klass.getName();
//		StringBuffer buf = new StringBuffer( kname.substring( kname.lastIndexOf('.')+1 ) );
		StringBuffer buf = new StringBuffer( kname );
		StringBuffer res = new StringBuffer();
		
		for ( int i=0; i<tabchars-2; i++ ) {
			if ( i == buf.length() ) res.append(' ');
			else if ( i > buf.length() ) res.append('-');
			else res.append( buf.charAt(i) );
		}
		
		res.append("> ");

		prefix = res.toString();
	}
	
	public static void toggleTimeStampLogging( boolean isOn ) {
		isLogTimeStamps = isOn;
	}
	
	public void logRaw( String s ) {
		printStream.println( s );
	}
	
	private void logPrivate( String s ) {

		if ( isLogTimeStamps ) {
//			date.setTime( System.currentTimeMillis() );
//			printStream.println( sdf.format( date ) + " " + prefix + s );
			// Cannot use same date as method cd be multithreaded and we want to avoid having to synchronize
			// Note: shift nanotime 21 to the right to obtain just the 6 digit microsecond precision.
			long millis = System.currentTimeMillis();
			long nanos = System.nanoTime();
//			printStream.println( sdf.format(new Date(millis)) + "~" + nanos + " " + prefix + s );
//			printStream.println( sdf.format(new Date(millis)) + "." + ((nanos<<1)>>1) + " " + prefix + s );
//			printStream.println( sdf.format(new Date(millis)) + "." + ((nanos<<2)>>2) + " " + prefix + s );
			
			// Keep this formatting!! - it is easily consumable by Derby: i.e. this works: values timestamp('2012-07-03 15:53:53.187')
			printStream.println( sdf.format(new Date(millis)) + "~" + (nanos%1000000000) + " " + prefix + s ); // Get a precision of 10^9, to increase likelihood of seeing a useful variation

//			printStream.println( sdf.format(new Date(millis)) /*+ "~" + (nanos%1000000000)*/ + " " + prefix + s );
		} else
			printStream.println( s );
		
//		printStream.println( "---------------> " + s );
//		printStream.println( System.currentTimeMillis() + " " + prefix + s );
//		printStream.println( new Date( System.currentTimeMillis() ).toGMTString() + " " + prefix + s );
//		printStream.println( new Timestamp(System.currentTimeMillis()) + " " + prefix + s );
	}
	
//	private void logThreadInfoPrivate( String s ) {		
//		logInfoPrivate( Thread.currentThread().getName() + ": " + s );
//	}
	
	private String getThreadInfo() {
		return Thread.currentThread().getName() + ": ";
	}
	
	/////////////////////////////////////////////////////////////////
	
	public static void setPrintStream( PrintStream ps ) {
		printStream = ps;
	}
	
//	Attempt to wrap this class around a java Logger to add a handler to limit the size of the log file.
//	public static void setJavaLogger( String file ) {
//		java.util.logging.Logger jlogger = java.util.logging.Logger.getLogger(file);
//		FileHandler handler = new FileHandler("myapp.log", 1000, 1);
//		jlogger.addHandler(handler);
//	}
	
	public static void setLogLevel( int level ) {
		logLevel = level;
	}
	
	public static boolean setLogLevel( String level ) {
		int lvl = getLogLevel(level);
		if ( -1 == lvl ) return false;
		logLevel = lvl;
		return true;
	}
	
	public static boolean isValidLogLevel( String level ) {
		return -1 != getLogLevel(level);
	}
	
	/**
	 * @param level
	 * @return true if succeeded, false otherwise (i.e. invalid String) 
	 */
	private static int getLogLevel( String level ) {
		
		for (int i=0; i<POSSIBLE_LEVELS.length; i++)
			if ( POSSIBLE_LEVELS[i].equalsIgnoreCase(level) ) return i;
		
		return -1;
	}
	
	
	/////////////////////////////////////////////////////////////////
		
	public void log( String s, int threshold ) {
		
		if ( threshold <= logLevel )
		switch ( threshold ) {
			case LOG_NONE:	logPrivate( WARNING_PREFIX + s + "\n" ); break;
			default: if ( Logger.LOG_ALL > logLevel && s.startsWith(LOG_EXCLUDE) ) return; logPrivate( s );
		}
	}
	
	public static String[] getLatestWarnings() {

		synchronized ( distinctWarnings ) {
			String[] warnings = distinctWarnings.values().toArray( new String[0] );
//			userWarnings.clear();
//			distinctUserWarnings.clear();
//			removingOldestWarnings = false;
			return warnings;
		}
	}
	
	// Consider warnings as the same if they match or if they only have differing digit sequences following a non-word character
    private static final Pattern numberAfterNonCharacter = Pattern.compile("(\\W)\\d+");
    private static final Map<String, String> distinctWarnings = new HashMap<String, String>();
	
	private static int getWarningRepeatCountAndRememberItsDetails( final String msg ) {
		
		final String warningWithoutNumbers = numberAfterNonCharacter.matcher(msg).replaceAll("$1").intern();
		final String timeNowString = new Timestamp(System.currentTimeMillis()).toString();
		int repeatCount = 1;
		
		synchronized ( distinctWarnings ) {
			
			String w = distinctWarnings.get( warningWithoutNumbers );
			
			if ( null == w )			
				w = timeNowString + 'x' + repeatCount + '~' + timeNowString + '#' + msg;
			else {
				int idx1 = w.indexOf('x'), idx2 = w.indexOf('~');
				
				repeatCount += Integer.parseInt( w.substring(idx1+1, idx2) );
				final String firstOccurenceTimeAndMessage = w.substring(idx2);
				
				w = timeNowString + 'x' + (repeatCount+1) + firstOccurenceTimeAndMessage;
			}
			
			distinctWarnings.put( warningWithoutNumbers, w );			
		}

		return repeatCount;
	}
	
//	private static LinkedList<String> userWarnings = new LinkedList<String>(), distinctUserWarnings = new LinkedList<String>();
//	private static boolean removingOldestWarnings = false;
//	private static void pushWarning( String w, int repeatCount ) {
//		
//		synchronized( userWarnings ) {
//		
//			userWarnings.addFirst(new Timestamp(System.currentTimeMillis()) + "#" + w);
//			if ( 1 == repeatCount )
//				distinctUserWarnings.addFirst(new Timestamp(System.currentTimeMillis()) + "#" + w); // delimit using '#' - this is split later
//			
//			if ( removingOldestWarnings ) userWarnings.removeLast();
//			else removingOldestWarnings = userWarnings.size() >= 100;
//		}
//	}
//	
//	private static int getWarningRepeatCount( final String warningWithoutNumbers ) {
//		int repeatCount = 0;
//		
//		synchronized( userWarnings ) {
//			for ( String s : userWarnings )
//				repeatCount += numberAfterNonCharacter.matcher(s.substring( s.indexOf('#')+1 )).
//					replaceAll("$1").intern() == warningWithoutNumbers ? 1 : 0; //Util.getStringSimilarityPercentage(w, s) > 90 ? 1 : 0;	
//		}
//		
//		return repeatCount;
//	}

	private static String expandAndSaveWarning( String codeIn, String message, Throwable e ) {
		String code = null != codeIn ? codeIn : null==e ? UNKNOWN_WARNING : UNKNOWN_ERROR;
		
		// Warning queue
		String msg = code + ": " + message + (null==e?"":e);
		
//		final String warningWithoutNumbers = numberAfterNonCharacter.matcher(msg).replaceAll("$1").intern();
//		int repeatCount = getWarningRepeatCount( warningWithoutNumbers ) + 1;
//		pushWarning( msg, repeatCount ); //msg + (null==e?"":e) );

		final int repeatCount = getWarningRepeatCountAndRememberItsDetails( msg );
		
		String repeat = "";
		if ( 9 < repeatCount ) {
			if ( 10 == repeatCount ) repeat = " (10th repetition)";
			else if ( 0 == repeatCount % 100 ) repeat = " (" + repeatCount + "th repetition)";
			else return null; // avoid repeatedly logging the same warning
		}

		// Log Message
		String doc = codeIn == null ? "" : "Documentation: " + findDocumentation(code);		
		long millis = System.currentTimeMillis();
		
		return 	"WARNING:\n\n" +
				sdf.format(new Date(millis)) + ' ' + "********** " + (null==e?"GDB_WARNING: ":"GDB_ERROR: ") + code + repeat + ": " + message
				+ ( null != e ? Util.getExceptionAsString(e) : "\n" ) + "\n" +
				sdf.format(new Date(millis)) + ' ' + doc +
				"\n\n";
	}

	public void logThreadException( String errorCode, String message, Exception e) {
		String digest = expandAndSaveWarning(errorCode, message, e);
		if ( null != digest ) logPrivate( getThreadInfo() + digest);
	}
	
	public void logThreadWarning( String warningCode, String message) {
		String digest = expandAndSaveWarning(warningCode, message, null);
		if ( null != digest ) logPrivate( getThreadInfo() + digest); // Always log these
	}
	
	public void logThreadAlways( String s ) {
		logPrivate( getThreadInfo() + s );
	}
	
	public void logThreadImportant( String s ) {
		if ( Logger.LOG_LESS <= logLevel ) {
			s = getThreadInfo() + s;
			if ( Logger.LOG_ALL > logLevel && s.startsWith(LOG_EXCLUDE) ) return;
			logPrivate( IMPORTANT_PREFIX + s );
		}
	}
	
	public void logThreadInfo( String s ) {
		if ( Logger.LOG_MORE <= logLevel ) {
			s = getThreadInfo() + s;
			if ( Logger.LOG_ALL > logLevel && s.startsWith(LOG_EXCLUDE) ) return;
			logPrivate( s );
		}
	}
	
	public void logThreadDetail( String s ) {
		if ( Logger.LOG_ALL == logLevel )
			logPrivate( getThreadInfo() + s );
	}
	
	/**
	 * Use this for unexpected errors -
	 * Use logWarning() instead for known conditions which can be logged fully in the warning text.
	 * logException() will print a stack trace which will make it easier to diagnose the issue. 
	 *  
	 * @param errorCode
	 * @param message
	 * @param e
	 */
	public void logException( String errorCode, String message, Throwable e) {
		String digest = expandAndSaveWarning(errorCode, message, e);
//		logPrivate("\n\n==>>>>         Pushed warning: " + warningCode + ": " +  message + e + "\n\n");
		if ( null != digest ) logPrivate( digest); // Always log these
	}
	
	/**
	 * Use logWarning() for known conditions which can be logged fully in the warning text.
	 * Use logException() instead for unexpected errors - this will print a stack trace which will make it easier to diagnose the issue. 
	 *  
	 * @param errorCode
	 * @param message
	 * @param e
	 */
	public void logWarning( String warningCode, String message) {
		String digest = expandAndSaveWarning(warningCode, message, null);
//		logPrivate("\n\n==>>>>         Pushed warning: " + warningCode + ": " +  message + "\n\n");
		if ( null != digest ) logPrivate( digest); // Always log these
	}

	public void logAlways( String s ) {
		logPrivate( s );
	}
	
	public void logImportant( String s ) {
		if ( Logger.LOG_LESS <= logLevel ) {
			if ( Logger.LOG_ALL > logLevel && s.startsWith(LOG_EXCLUDE) ) return;
			logPrivate( IMPORTANT_PREFIX + s );
		}
	}
	
	public void logInfo( String s ) {
		if ( Logger.LOG_MORE <= logLevel ) {
			if ( Logger.LOG_ALL > logLevel && s.startsWith(LOG_EXCLUDE) ) return;
			logPrivate( s );
		}
	}
	
	public void logDetail( String s ) {
		if ( Logger.LOG_ALL == logLevel )
			logPrivate( s );
	}

	public static String findDocumentation(String code) {

		if ( code.equals(UNKNOWN_WARNING) ) return "<unknown error/warning code>";
		
		final String defaultPathPrefix = "<GaianDBDocumentationDirectory>/";
		final String relativeDocPath = "javadoc-errors/com/ibm/gaiandb/diags/GDBMessages.html";
		
		String installPath = Util.getInstallPath();
		if ( null == installPath ) {
			logger.logInfo("findDocumentation() unable to locate javadoc due to unresolved install path - path prefix will be masked");
			return defaultPathPrefix + relativeDocPath + "#" + code;
		}
		
		String docPath = installPath + "/doc/" + relativeDocPath;
		
//		System.out.println("installPath: " + installPath + ", docPath: " + docPath + ", docPathExists: " + new File(docPath).exists());
		
		return new File(docPath).exists() ?
				"file://" + ( Util.isWindowsOS ? "/" : "" ) + docPath + "#" + code :
				defaultPathPrefix + relativeDocPath + "#" + code;
	}
}
