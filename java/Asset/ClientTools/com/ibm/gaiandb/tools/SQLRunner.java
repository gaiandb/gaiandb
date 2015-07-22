/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.tools;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import com.ibm.gaiandb.CachedHashMap;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianNode;
import com.ibm.gaiandb.Util;


/**
 * @author DavidV
 */
public class SQLRunner {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	// Caching prepared statements in the client may marginally speed up queries... BUT:
	// ... it will also mean VTI objects are not closed so readily meaning their cached rows will not be freed up
	private boolean CACHE_PREPARED_STATEMENTS = false;
	public void setCachePreparedStatements( boolean doCache ) { CACHE_PREPARED_STATEMENTS = doCache; }
	
    protected final String DEFAULT_USR;
    protected final String DEFAULT_PWD;
    protected final String DEFAULT_HOST;
    protected final int DEFAULT_PORT;
    protected final String DEFAULT_DATABASE;
    
	protected String mUsr = null;
	protected String mPwd = null;
	protected String mHost = null;
	protected int mPort = -1;
	protected String mDatabase = null;
	protected String batchPrefix = "";
	
	private boolean dev = false;
	private boolean showtimes = false;
	
	private int repeat = 1;
	
	protected String url = null;
	
	// The next 2 args only really apply to Derby (at the moment)
	protected static boolean standalone=false;
	protected static boolean createdb=false;
	protected static boolean upgrade=false;
	protected static String sslMode=null;
	
	protected PrintStream printStream = System.out;
	public void setPrintStream( PrintStream ps ) { printStream = ps; }
    
    private BufferedReader stdin = new BufferedReader( new InputStreamReader( System.in ) );
	
//	private void println() { print("\n"); }
//	private void println( String msg ) { print( msg + "\n" ); }
//	private void println( StringBuffer msg ) { print( msg + "\n" ); }
//	private void print( String msg ) { if ( null == printStream ) System.out.println(msg); else printStream.println(msg); }
	
	// Prepared Statements Cache: Note the SQLRunner is not currently designed to execute 
	// mutliple queries concurrently.. this would require the use of a Connection Pool, and each query
	// could executed by a prepared statement prepared against any of the available connections.
	// Note this must not be static - each instance has its own set of prepared stmts...
    private CachedHashMap<String, PreparedStatement> preparedStatements = new CachedHashMap<String, PreparedStatement>(100); // Not used because logical table definitions are dynamic. (Statements are cached on server though)
    
    // SQLRunner connection - this is the JDBC Connection against which the statements are prepared
    private Connection instanceConn = null;
    
    private int[] summaryResults = null; // Returned off some methods when we are called programmatically
    private static final int CREATES=0, DROPS=1, INSERTS=2, DELETES=3, UPDATES=4, SELECTS=5, CALLS=6, OTHERS=7;
    
    private String lastSQL = null; 
    
	private boolean exitOnFailure = true;

	protected String USAGE;
	
	protected static final String BASE_ARGS = "[-h <host>] [-d <database>] [-p <port>] [-usr <usr>] [-pwd <pwd>] "
		+ "[-td[<delimiter>]] [-t] [-tab] [-csv] [-raw] [-quiet] [-showtimes] [-repeat <count>] [-batchprefix <sql fragment>]";
	protected static final String COMMON_USAGE =
			"\n-td[delimiter]: Toggle SQL statement delimiter char. If '-td' has no appended character, the delimiter becomes '\\n'" +
			"\n-t:  This sets the SQL delimiter to ';'. This shortcut for '-td;' avoids interfeering with shell interpretation of semi-colon" +
    		"\n-tab: Output results in default table format which is with table headings and vertical line separators" +
    		"\n-csv: Output results in csv format" +
    		"\n-raw: Output results as raw data, space-separated format (no info or headers)" +
    		"\n-quiet: No output to stdout (except if the -repeat option is also used, in which case just the cumulated results are displayed)" +
    		"\n-repeat <count>: Specify a number of times the query should be re-issued, cumulated results are displayed." +
    		"\n-showtimes: Show a cummulative summary of performance metrics (only useful with -repeat)." +
			"\n-batchprefix <sql fragment>: Specify a SQL fragment to insert as prefix to every SQL statement"
    		;
	
	protected boolean csv=false, raw=false, quiet=false;
	
	private char delimiterChar=';'; // Default delimiter ';' for multi-sql cmds and files - for backwards compatibility
	private boolean isDelimiterSet = false;
	private boolean isDefaultBackwardCompatibilityMode = true; // false removes special meaning for '\', '#' and empty lines in statements 
    
	public SQLRunner( Connection c ) { this(null, null, null, -1, null); instanceConn = c; url = "nonNullDummyString"; }
	
	protected SQLRunner( String defUsr, String defPwd, String defHost, int defPort, String defDb ) {
		DEFAULT_USR = defUsr;
		DEFAULT_PWD = defPwd;
		DEFAULT_HOST = defHost;
		DEFAULT_PORT = defPort;
		DEFAULT_DATABASE = defDb;
	}
	
	protected SQLRunner( String defUsr, String defPwd, String defHost, int defPort, String defDb, boolean exitOnFailure ) {
		this(defUsr, defPwd, defHost, defPort, defDb);
		this.exitOnFailure = exitOnFailure;
	}
		
    //==============================================================================================
    // ResultSet display
    //==============================================================================================
    private static String allblanks = "                                                         ";
    private static String delimiters =  "===============================================================================";
    
    /*
     * automatically pad characters at end of string, try to buffer padded strings to not recreate them everytime.
     */
    private static String pad(int lg,char c)
    {
        if (lg<=0) return "";
        if (c==' ' && lg <= allblanks.length()){  //printInfo("reusing allblanks");
            return allblanks.substring(1,lg);
        }
        else
        if (c=='=' && lg <= delimiters.length()){ //printInfo("reusing delimiters");
            return delimiters.substring(1,lg);
        }
        else {
            StringBuffer s=new StringBuffer("");
            for (int i=1; i<lg; i++)
                s.append(c);
    //      if (allblanks.length()<lg)
    //      {
    //          allblanks=s.toString();
    //          printInfo("replacing allblanks with length"+allblanks.length()+"with a string o f length "+lg);
    //      }
            return s.toString();
        }
    }

    /**
     * Proces results and display them to the right output with basic formatting
     * @param rs : the result set to fetch and display
     * @return the number of rows processed in this result set
     * @throws Exception
     */
    public int processResultSet(ResultSet rs) throws Exception
    {
        int nbrows=0;
    	if ( quiet ) {
    		while ( rs.next() ) nbrows++;
    		return nbrows;
    	}
    	
        int maxcollg=1024;
        
//        NumberFormat nf = NumberFormat.getInstance();
        char ColDel = csv ? ',' : raw ? ' ' : '|';
        ResultSetMetaData rsmd = rs.getMetaData();
        StringBuffer row = new StringBuffer("");
        StringBuffer types = new StringBuffer("");
        int[] lg=new int[rsmd.getColumnCount()+1];
        int[] formatting=new int[rsmd.getColumnCount()+1];
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
        {
            String name=rsmd.getColumnName(i);
            lg[i]=(rsmd.getColumnDisplaySize(i));
            formatting[i]=( rsmd.getColumnType(i));
            //if (lg[i] > maxlg)    lg[i]=maxlg; 
            if (name.length()> lg[i]) lg[i]=name.length();
            if (lg[i] > maxcollg) lg[i]=maxcollg;
            row.append(name+pad(lg[i]-name.length()+1,' ')+ColDel);
            types.append(""+formatting[i]+pad(lg[i]-(""+formatting[i]).length()+1,' ')+ColDel);
            //printInfo("col"+i+"="+rsmd.getColumnDisplaySize(i) ) ;
        }           
        String delimiter=pad(row.length()+1,'=');
        
        printInfo( csv ? "" : delimiter);
        printInfo(row.toString());
        //if (tracelevel==1) printInfo((types);
        if (!csv) printInfo(delimiter);

//        Little block that shows SQL to Java type correspondence
//        if ( !rs.next() ) return 0;
//        for (int i=1; i<= rsmd.getColumnCount(); i++){        	
//        	Object o = rs.getObject(i);
//        	printStream.println("col" + i + ": " + (o instanceof byte[] ? "byte[]" : o.getClass().getName()));
//        }
//        if ( true ) return 1;
        
        while (rs.next()) {
        	
//    		if ( dev ) {
//    			printStream.println("[fetched 1 row [press return to continue]> ");
//    			stdin.readLine();
//    		}
        	
        	StringBuilder Datarow=new StringBuilder("");
            int numCols = rsmd.getColumnCount();
            
            if ( 1 == numCols && raw ) Datarow.append( rs.getString(1) ); // no formatting required if just 1 column in raw mode...
            else
	            for ( int i=1; i <= numCols; i++ ) {
	            	
	            	String colvalue = rs.getString(i);
//	            	String colvalue = null;
//	            	Object o = rs.getObject(i);
//	            	printStream.println("Object retrieved is of type: " + o.getClass().getName());
//	            	if ( o instanceof Blob ) {
//	            		InputStream is = ((Blob)o).getBinaryStream();
//	    				ByteArrayOutputStream baos = new ByteArrayOutputStream();    				
//	    				Util.copyBinaryData(is, baos);
//	    				colvalue = Util.byteArray2HexString( baos.toByteArray(), false );
//	    				is.close(); baos.close();
//	            	} else {
//	                    colvalue=rs.getString(i);
//	            	}
	            	
	                if (colvalue == null) colvalue= raw ? "" : "-";
	                switch (formatting[i])
	                {
	                //right formatting
	                case -5:
	                case 5:
	                case 4:
	                case 3: Datarow.append(pad(lg[i]- colvalue.length()+1,' ')+colvalue+ColDel);                       
	                        break;
	                
	                //left formatting
	                default:if (csv)
	                             Datarow.append('"'+colvalue+pad(lg[i]- colvalue.length()+1,' ')+'"'+ColDel);
	                        else Datarow.append(colvalue+pad(lg[i]- colvalue.length()+1,' ')+ColDel);
	                }
	            }
            
            if (!quiet) printStream.println(Datarow);
            nbrows++;
        }//endwhile         
        
        printInfo( csv ? "" : delimiter);
        
		SQLWarning nextWarning = rs.getWarnings();
		
		Vector<SQLWarning> warnings = new Vector<SQLWarning>();
		
		while ( null != nextWarning ) {
		    warnings.add( nextWarning );
		    nextWarning = nextWarning.getNextWarning();
		}
		
		if ( 0 < warnings.size() ) printInfo("ResultSet Warnings: " + warnings);
		
        return nbrows;
    }
        
    /**
     * Returns the number of args left after processing dash '-' arguments.
     * @throws Exception 
     */
	protected int setArgs(String[] args) {
		
		int argsRemaining = 0;
		
		upgrade=false;
		
		for( int i=0; i<args.length; i++ ) {
			
			final String arg = args[i];
			final int arglen = arg.length();
			
//			printInfo("arg: " + arg); // + ", val: " + val);
			try {
				if ( this instanceof SQLDerbyRunner && "-standalone".equals( arg ) ) { url = null; standalone=true; }
				else if ( this instanceof SQLDerbyRunner && "-createdb".equals( arg ) ) { createdb=true; url = null; }
				else if ( this instanceof SQLDerbyRunner && "-nocreatedb".equals( arg ) ) { createdb=false; url = null; }
				else if ( "-dev".equals( arg ) ) { dev=true; }
				else if ( "-showtimes".equals( arg ) ) { if ( quiet ) showtimes=true; }
				else if ( "-quiet".equals( arg ) ) { 	showtimes=false; quiet=true; }
				else if ( "-tab".equals( arg ) ) { 		showtimes=false; quiet=false; repeat=1; raw=false; csv=false; }
				else if ( "-csv".equals( arg ) ) { 		showtimes=false; quiet=false; repeat=1; raw=false; csv=true; }
				else if ( "-raw".equals( arg ) ) { 		showtimes=false; quiet=false; repeat=1; raw=true;  csv=false; }
				else if ( "-csvraw".equals( arg ) ) { 	showtimes=false; quiet=false; repeat=1; raw=true;  csv=true; }
				else if ( "-upgrade".equals( arg ) ) { url = null; upgrade=true; }
				else if ( "-t".equals(arg) ) { isDelimiterSet = true; delimiterChar = ';'; isDefaultBackwardCompatibilityMode = false; }
				else if ( arg.startsWith("-td") && arglen < 5 ) {
					isDefaultBackwardCompatibilityMode = false;
					if ( 3 == arglen) { isDelimiterSet = false; }
					else if ( 4 == arglen ) { isDelimiterSet = true; delimiterChar = arg.charAt(3); }
//					printStream.println("del: " + delimiterChar);
				}
				else if ( arg.startsWith("-ssl=") ) { sslMode = arg.substring("-ssl=".length()); }
				else if ( ! arg.startsWith("-") ) { argsRemaining = args.length - i; break; }
				else {
					
					// There must be a value for this argument
					if ( i == args.length-1 ) syntaxError("Unexpected argument or missing value for argument: " + arg);
					
					String val = args[++i];
					
					if ( "-p".equals( arg ) ) {
//						if ( /*mPort != -1 ||*/ standalone ) syntaxError("Option '-p' is incompatible with '-standalone'"); //can only be specifed once and 
						standalone=false;
						int port = Integer.parseInt(val);
						
						if ( port != mPort ) {
							mPort = port;
							
							// If this is a DerbySQLRunner (i.e. default gaiandb db) and th database name hasnt been set or was set to
							// a gaiandb<port> database name, then implicitely set the db name aswell.
							if ( GaianDBConfig.GAIANDB_NAME.equals(DEFAULT_DATABASE) && 
									( null == mDatabase || mDatabase.startsWith(DEFAULT_DATABASE) ) )
								mDatabase = DEFAULT_DATABASE + ( DEFAULT_PORT == mPort ? "" : mPort );
							
							url = null; // triggers this SQLRunner instance to re-connect
						}
					}
					else if ( "-usr".equals( arg ) ) { mUsr = val; url = null; }
					else if ( "-pwd".equals( arg ) ) { mPwd = val; url = null; }
					else if ( "-h".equals( arg ) ) { standalone=false; mHost = val; url = null; }
					else if ( "-d".equals( arg ) ) { mDatabase = val; url = null; }
					else if ( "-batchprefix".equals( arg ) ) { batchPrefix = val; } 
					else if ( "-repeat".equals( arg ) ) { repeat = Integer.parseInt( val ); }
					else { syntaxError("Unexpected argument: " + arg); }
				}
			} catch ( Exception e ) {}
		}
		
		ensureConnectionPropertiesAreNowSet();
		
//		printStream.println("argsRemaining = " + argsRemaining);
		
		return argsRemaining;		
	}
	
	private void ensureConnectionPropertiesAreNowSet() {
		if ( -1 != mPort ) {
			if ( null == mDatabase )
				mDatabase = DEFAULT_DATABASE + mPort;
		} else
			mPort = DEFAULT_PORT;
		
		if ( null == mDatabase ) mDatabase = DEFAULT_DATABASE;
		if ( null == mHost ) mHost = DEFAULT_HOST;
		if ( null == mUsr ) mUsr = DEFAULT_USR;
		if ( null == mPwd ) mPwd = DEFAULT_PWD;
	}
	
	protected void printInfo( String s ) {
		if ( !quiet && !raw ) printStream.println(s);
	}
    
	protected void syntaxError(String help) throws Exception {
		
		if ( exitOnFailure ) {
			printStream.println( "\n" + help + "\n" + USAGE + "\n" );
			System.exit( 1 );
		} else {
			printStream.println( "Syntax Error: " + help );
			throw new Exception( help );
		}
	}
    
    /**
     * Returns the number of args left after processing dash '-' arguments.
     */
//	abstract protected int setArgs(String[] args);
	
	/**
	 * Takes the Class name of the SQLRunner to run as first argument.
	 * 
	 * @param args
	 * @throws Exception
	 */
    public static void main(String[] args) throws Exception {
    	
    	String[] supportedRDBMSs = { "Derby", "DB2", "MySQL", "SQLServer" };
    	String usage = "USAGE: " + SQLRunner.class.getSimpleName() + " " + Arrays.asList(supportedRDBMSs);
    	
    	if ( 1 > args.length ) throw new Exception( usage );
    	String arg = args[0];
    	
    	for ( int i=0; i<supportedRDBMSs.length; i++ ) {
    		if ( arg.equals(supportedRDBMSs[i]) ) {
		    	String[] argsShifted = new String[args.length-1];
		    	System.arraycopy( args, 1, argsShifted, 0, argsShifted.length );
		    	((SQLRunner) Class.forName( "com.ibm.gaiandb.tools.SQL" + arg + "Runner" ).newInstance()).processArgs( argsShifted );
		    	
		    	System.exit(0);
    		}
    	}
    	
    	throw new Exception( usage );
    }    

	public void processArgs( String[] args ) throws SQLRunnerException {
		try {
			processArgsWithoutClosingConnection(args);
		} catch ( Exception e ) {
			final String iex = Util.getGaiandbInvocationTargetException(e);
			System.out.println("Unable to process arguments: " + Arrays.asList(args) + ", cause: "
					+ e.getMessage() + (null==iex?"":" Root cause: "+iex));
			if ( -1 != Util.getStackTraceDigest().indexOf("Test") ) // don't exit out of junit tests...
				System.exit(1);
		} finally {
			if ( null != instanceConn )
				try { instanceConn.close(); }
				catch (SQLException e) {
					printInfo("processArgs() unable to close() connection: " + e.getMessage());
					e.printStackTrace();
				}
		}
	}
    
	/**
	 * Process command line args and run queries
	 * 
	 * @param args
	 * @throws SQLRunnerException 
	 */
	public void processArgsWithoutClosingConnection( String[] args ) throws SQLRunnerException {
		
//		printStream.println("Processing " + args.length + " arguments: " + Arrays.asList( args ));
//		for ( int i=0; i<args.length; i++)
//			printStream.println("arg " + i + " = " + args[i] + ", arg has length " + args[0].length());
		
		// Cater for case where an empty 1st arg is passed in.
		if ( 1 == args.length && 0 == args[0].length() ) args = new String[0];
		
		List<String> largs = Arrays.asList(args);
		if ( !raw && !largs.contains("-raw") && !largs.contains("-csvraw") ) {
			String summary = largs.toString(); int idx = summary.indexOf('\n');
			printStream.println("Processing " + largs.size() + " args: " 
					+ (0 > idx ? summary : summary.substring(0, idx) + "(continued...)]"));
		}
		
//    	if ( args.length < 1 ) syntaxError();
		int argsRemaining = setArgs( args );
//    	String sqlOrFile = args[ args.length-1 ];
    	
		exitOnFailure = false;
//		printStream.print("isDelimiterSet: " + isDelimiterSet + ", delimiterChar: " + delimiterChar);
		
    	try {
    		
    		if ( 0 == argsRemaining ) {
    			// No SQL or file path were passed in - so we enter interactive mode
    			
    	        StringBuilder sqlbuf = new StringBuilder();
    	        String sql = null;
    	        
    	        while (true) {
    	            
	            	if ( null != sql ) {
	    	        	
	    	        	if ( 1 > sqlbuf.length() ) {
	    	        		
	    	        		// ready() Returns true if the next read() is guaranteed not to block for input, false otherwise.
	    	        		if ( false == stdin.ready() ) // this stops us printing "sql> " multiple times on the final line...
	    	        			printStream.print("sql> ");
		    	        	
		    	        	String newline = stdin.readLine();
		    	        	// Don't allow piped user input
		    	        	if ( null == newline ) break;

		    	        	newline = newline.replaceFirst("^[\\s]*", ""); // trim leading spaces (not ending ones!)
		    	            if ( isDelimiterSet ) // remove leading delimiter chars (if we have a delimiter for interactive mode)
		    	            	newline = newline.replaceFirst("^["+delimiterChar+"\\s]*", "");

		    	            if ( 1 > newline.length() || '#' == newline.charAt(0) || matchAndSetArgs(newline) ) continue;
		    	            
		    	            if ( "quit".equalsIgnoreCase(newline) || "exit".equalsIgnoreCase(newline) ) break;

		    	        	sqlbuf.append( newline );
	    	        	}
	    	        	
	    	        	sql = resolveStatementToNextDelimiter( stdin, sqlbuf, isDelimiterSet );
	            	}
    	        	
    	            while (true) {
    	            	
    	            	if ( null == instanceConn || instanceConn.isClosed() ) {
    	            		
    	            		if ( null != sql ) printStream.println("\nAttempting to re-connect...");
    	            		
    	            		try { instanceConn = getConnection(); }
    	            		catch ( Exception e1 ) {
    	            			printStream.println("Connection attempt failed: " + e1);
    	            			if ( -1 == e1.toString().toLowerCase().indexOf("authentication") && ! (this instanceof SQLDiscoveryClientRunner) )
    	            			printStream.println("Make sure the database is not booted by another instance in standalone mode " +
    	            					"and that the server is running on the expected port (or switch to -standalone for this instance). " +
    	            					"Check that you have appropriate access privileges for this database (e.g. if derby.database.defaultConnectionMode=noAccess in derby.properties)."+
    	            					"Also check the Derby jars are the correct version. Enter a SQL query to re-connect.\n");
    	            			break;
    	            		}
    	            		
    	            		if ( null == sql ) break;
    	            		
    	            		printStream.print("Connection attempt succeeded, Re-run query [Y/n] ? ");
    	            		while ( stdin.ready() ) stdin.readLine(); // throw away all pending lines
    	            		try { String s = stdin.readLine(); if ( s.equalsIgnoreCase("n") ) break; }
    	            		catch ( Exception e2 ) { break; } // shouldnt happen
    	            	}
    	            	
	    	            try { processSQLs( sql, batchPrefix, false ); break; }
	    	            catch ( Exception e ) {
	    	            	
//	    	            	e.printStackTrace();

	    	    			List<String> messages = new LinkedList<String>();
	    	    			messages.add(e.getMessage());
	    	    			
	    	    			boolean isSQLSyntaxException = e instanceof SQLSyntaxErrorException;

	    					printStream.println( "\nChained Exception 1: "+e ); int i=1;
	    	    			Throwable cause = e;
	    	    			while (null!=(cause=cause.getCause())) {
	    	    				++i;
	    	    				isSQLSyntaxException = isSQLSyntaxException || cause instanceof SQLSyntaxErrorException;
	    	    				if (!messages.contains(cause.getMessage())) {
	    	    					messages.add(cause.getMessage());
	    	    					printStream.println( "Chained Exception " + i + ": " + cause );
	    	    					continue;
	    	    				}
	    	    				printStream.println( "..." );
	    	    			}
	    	    			
	    	            	if ( e instanceof SQLException && -1 != e.getMessage().toLowerCase().indexOf("socket") ) {
		    	            	if ( null != instanceConn && !instanceConn.isClosed() ) instanceConn.close();
	    	            		continue;
	    	            	}
	    	            	
	    	            	printStream.println();
	    	            	
    	            		if ( this instanceof SQLDerbyRunner && ! isSQLSyntaxException ) {
		    	            	// Re-cycle the connection in case its become corrupt
		    	            	printStream.println("\nRecycling Connection...");
		    	            	if ( null != instanceConn && !instanceConn.isClosed() ) instanceConn.close();
	    	            		try { instanceConn = getConnection(); /*stmt = c.createStatement();*/ }
	    	            		catch ( Exception e1 ) { continue; } // If this fails then loop back to attempting re-connects

	    	            		while ( stdin.ready() ) stdin.readLine(); // throw away all pending lines
	    	            		printStream.print("List Server Warnings [y/N] ? "); String choice = stdin.readLine();
	    	            		if ( null != choice && ( 0 < choice.length() && 'Y' == choice.toUpperCase().charAt(0) ) ) {
			    	            	printStream.println("Listing Server Warnings...");
		    	            		try { executeSQL( "call listwarningsx(0)" ); }
		    	            		catch ( Exception e1 ) { printStream.println("call listWarningsx(0) failed: " + e1); }
	    	            		}
	    	            		
//	    	            		printStream.print("Re-run query [Y/n] ? "); choice = stdin.readLine();
//	    	            		if ( null != choice && ( 0 == choice.length() || 'N' != choice.toUpperCase().charAt(0) ) )
//	    	            			continue;
    	            		}
    	            		
    	            		// We have given up on retries..
    	            		while ( stdin.ready() ) stdin.readLine(); // throw away all pending lines
    	            		sqlbuf.setLength(0); // clear next sql fragments
    	            		break;
	    	            }
    	            }
    	            
    	            sql = "";
    	        }
    		} else {
    			// batch mode (non-interractive) - 1 or more queries were passed in directly
    			instanceConn = getConnection();
        		/*stmt = c.createStatement();*/
        		for (int i=0; i<argsRemaining; i++)
        			processSQLs( args[args.length-(argsRemaining-i)], batchPrefix, true );
    		}
    		
		} catch (Exception e) {
			// Don't print a stack digest to queryDerby.bat - this is unreadable to users - Also it breaks test: logger.Test_setltForExcelException
			// Also an exception here often won't carry the server side exception info..
			printInfo("Caught Exception: " + e.getMessage());
			if ( exitOnFailure )
				System.exit(1);
			else
				throw new SQLRunnerException(e);
		}
	}
	
	private boolean matchAndSetArgs( final String input ) {
        // Match argument command: starts with "-", but not "--" which is a SQL comment)
    	if ( 1 < input.length() && '-' == input.charAt(0) && '-' != input.charAt(1) ) {
			setArgs( Util.splitByTrimmedDelimiter( input, ' ' ) );
			if ( null == url && null != instanceConn ) try { instanceConn.close(); } catch (Exception e) {};
			return true;
		}
    	
    	return false;
	}
	
	/**
	 * Behavior for backwards compatibility (default mode: no '-td' or '-t' arguments specified):
	 * 		- A trailing '\' char (optionally followed by white space) at the end of a line continues the line (without adding a new line char)
	 * 		- An empty line (or full of white space) always ends the current statement (irrespective of whether there is a defined delimiter) 
	 * 		- A leading hash char '#' at the start of any line (and optionally preceded by white space) designates a user comment and is skipped.
	 * 
	 * The 3 behaviors above are eliminated if one of the delimiter options was specified (-td or -t), i.e:
	 * 		- Trailing '\' and white space remains part of the query
	 * 		- Empty lines (or full of white space) remain part of a pending query statement (they do not end it) 
	 * 		- Leading '#' chars (and preceding white space) remain part of the query
	 * 
	 * Summary:
	 * Default mode (backwards-compatible) uses ';' as delimiter in scripts and gives special meaning to #, \ and empty lines.
	 * Arguments '-td' or '-t' set a consistent delimiter for script/batch/interactive modes, and remove special meaning for #, \, and empty lines
	 * 
	 * @param br
	 * @param sqlbuf
	 * @param useDelimiter
	 * @return
	 */
	private String resolveStatementToNextDelimiter( BufferedReader br, StringBuilder sqlbuf, boolean useDelimiter ) {
		
		// Read multiple lines of SQL until we have a full statement to return - then also remove the statememt from sqlbuf.
		try {
			String nxtLine = null;
			boolean isNewLineEscaped = false;
			
			while ( true ) {
				
    			if ( useDelimiter ) {
    				// Start looking for delimiters...
    				String sqlFrag = sqlbuf.toString();
    				String[] parsedSQLs = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrQuotes(
    						sqlFrag, delimiterChar, false, 2, "--", "\n" );
//        			printInfo("parsedSQLs[] = " + Arrays.asList(parsedSQLs) );
    				
    				int numElmts = parsedSQLs.length;
            		if ( 1 < numElmts ) {
           			// We have a new delimited statement to return
            			String sqlStmt = parsedSQLs[0], nxtStmtStart = parsedSQLs[1];
    					// Delete the newly found SQL statement from the buffer...
            			if ( 1 > nxtStmtStart.length() || '#' == nxtStmtStart.charAt(0) ) sqlbuf.setLength(0);
            			else sqlbuf.delete( 0, sqlbuf.indexOf(parsedSQLs[1], sqlbuf.indexOf(sqlStmt)+sqlStmt.length()) );
            			
//            			printInfo("Next SQL = " + sqlStmt );
//            			printInfo("Remaining sqlbuf = " + sqlbuf );
    					return sqlStmt;
    				}
    			} else if ( false == isDefaultBackwardCompatibilityMode ) break; // reached end of line - cannot be escaped with '\'
    			
    			if ( false == isDefaultBackwardCompatibilityMode ) nxtLine = br.readLine();
    			else {
    				// Default backwards-compatibility mode:
    				//	Empty or white space lines end queries; and '#' and '\' are special characters (when having empty spaces before/after)
    				
        			// Detect line ending with '\' with optional trailing white space
        			int lastidx = -1; char c;
        			for ( int i=sqlbuf.length()-1; i>=0; i-- )
        				if ( '\\' == ( c = sqlbuf.charAt(i) ) ) { lastidx = i; break; }
        				else if ( '\u0020' < c ) break;
        			
        			isNewLineEscaped = -1 < lastidx;
        			
//        			int lastidx = sqlbuf.length()-1;
//        			isNewLineEscaped = '\\' == sqlbuf.charAt(lastidx);
        			if ( isNewLineEscaped ) sqlbuf.setLength( lastidx ); // deletes everything from the '\' character
        			else if ( false == useDelimiter ) break; // Delimiter is the end of line..
        			
        			// Detect leading hash char '#' with optional leading white space
        			nxtLine = null;
        			while ( null == nxtLine ) {
        				nxtLine = br.readLine();
        				// Check for end of statement ( 1 > nxtLine.trim().length() disallows lines that are empty or have only white space )
        				if ( null == nxtLine || 1 > nxtLine.trim().length() ) { nxtLine = null; break; } // no more lines in this statement

            			for ( int i=0; i<nxtLine.length(); i++ )
            				if ( '#' == ( c = nxtLine.charAt(i) ) ) { nxtLine = null; break; } // skip this line
            				else if ( '\u0020' < c ) break; // this line is part of the statement
        			}
    			}
    			
    			if ( null == nxtLine ) break; // no more lines in this statement
				sqlbuf.append( (isNewLineEscaped?"":"\n") + nxtLine ); // don't trim() this... (might be inside a string value)
			}
		} catch ( Exception e ) { System.err.println("Unexpected exception while reading SQL lines: " + Util.getStackTraceDigest(e)); }
		
		// Query is delimited by a new line, or the BufferedReader is exhausted - return what's left
		String sqlStmt = sqlbuf.toString();
		sqlbuf.setLength(0);
		return sqlStmt;
	}
	
    public String processSQLs( String sqlOrArgsOrFile ) throws Exception {
    	if ( null == summaryResults ) summaryResults = new int[8];
    	for ( int i=0; i<summaryResults.length; i++ ) summaryResults[i] = 0;
		
    	try { processSQLs( sqlOrArgsOrFile, "", true ); }
		catch (SQLException e) { throw new SQLException(e + " SQL: " + lastSQL, e); }    	
    	
    	return  summaryResults[CREATES] + " CREATES, " + summaryResults[DROPS] + " DROPS, " + summaryResults[INSERTS] + " INSERTS, "
    		+	summaryResults[DELETES] + " DELETES, " + summaryResults[UPDATES] + " UPDATES, " + summaryResults[SELECTS] + " SELECTS, "
    		+	summaryResults[CALLS] + " CALLS, " + summaryResults[OTHERS] + " OTHERS";
    }
	
    private void processSQLs( String sqlOrArgsOrFile, final String batchPrefix, boolean isPossiblyMultipleDelimitedStatements ) throws Exception {
    	
		sqlOrArgsOrFile = sqlOrArgsOrFile.trim();
		BufferedReader bufferedReader = null;
		
    	// First assume this is a file... (as long as there is no ';' or '\n' in it - this avoids
		// the possible blue screen that occurs when we pass in a string that is too long - e.g. the
		// length of the whole SQL used to drop/create all the Stored Procedures defining the APIs.)
		if ( -1 == sqlOrArgsOrFile.indexOf(delimiterChar)  && -1 == sqlOrArgsOrFile.indexOf('\n') ) {
			final String gdbWorkspace = GaianNode.getWorkspaceDir();
			final String fPath = null == gdbWorkspace || Util.isAbsolutePath(sqlOrArgsOrFile) ? sqlOrArgsOrFile : gdbWorkspace+"/"+sqlOrArgsOrFile;
			try { bufferedReader = new BufferedReader( new FileReader( fPath ) ); } // batch file?
			catch (FileNotFoundException e1) {} // ignore - this means 'sqlOrArgsOrFile' is not a file.
		}
		
		if ( null == bufferedReader ) {
			// TODO: should be able to integrate matchAndSetArgs() with the double 'while' loop logic below..?
			if ( matchAndSetArgs( sqlOrArgsOrFile) ) return; // simple argument command (e.g. -p 6415 to switch to node running on 6415)
			
			if ( false == isPossiblyMultipleDelimitedStatements ) {
				// This can only be a single statement
				executeSQLRepeat( new StringBuilder( batchPrefix + sqlOrArgsOrFile ) );
				return;
			}
			
			// sqlOrArgsOrFile is a sequence of SQL queries, delimited by our delimiterChar
			bufferedReader = new BufferedReader( new StringReader( sqlOrArgsOrFile ) );
		}
		
		String buf = null;
		final StringBuilder nextSQLStatement = new StringBuilder( batchPrefix );
		final int batchPrefixLen = batchPrefix.length();
		
    	while ( null != (buf = bufferedReader.readLine()) ) {
    		
    		StringBuilder sql = new StringBuilder( buf.replaceFirst("^["+delimiterChar+"\\s]*", "") ); // remove leading spaces and delimiters
        	
        	while ( 0 < sql.length() && '#' != sql.charAt(0) ) {
	    		executeSQLRepeat( nextSQLStatement.append(
	    				// Use delimiter ';' by default for batch or script processing (i.e. when -td or -t where not specified)
	    				resolveStatementToNextDelimiter( bufferedReader, sql, isDelimiterSet || isDefaultBackwardCompatibilityMode ) ) );
	    		nextSQLStatement.setLength( batchPrefixLen );
        	}
    	}
    	
    	bufferedReader.close();
    }
    
    private long rttime = 0, retime = 0;
    private int rnumRows = 0;
    private void executeSQLRepeat( StringBuilder sql ) throws Exception {
    	
		// Double check if SQL is not null, empty or commented out - if not then execute
		if ( null == sql || 1 > sql.length() || '#' == sql.charAt(0) ) return;
			
		boolean suppressException = '!' == sql.charAt(0);
		if ( suppressException ) sql.deleteCharAt(0);
		
		final String[] sqltoks = Util.splitByWhitespace( sql.toString() );
		
		boolean isAutoCommitCommand = 1 < sqltoks.length && "AUTOCOMMIT".equals(sqltoks[0].toUpperCase());
					
		if ( isAutoCommitCommand && "ON".equals(sqltoks[1].toUpperCase()) ) instanceConn.setAutoCommit(true);
		else if ( isAutoCommitCommand && "OFF".equals(sqltoks[1].toUpperCase()) ) instanceConn.setAutoCommit(false);
		else if ( 0 < sqltoks.length && "COMMIT".equals(sqltoks[0].toUpperCase()) ) instanceConn.commit();
		else {
			lastSQL = sql.toString();
//			printInfo("Processing sql: " + sql);
	    	
	    	rttime = 0; retime = 0; rnumRows = 0;
	    	for ( int i=0; i<repeat; i++ )
	    		if ( suppressException )
	    			try { executeSQL( lastSQL ); }
	    			catch ( Exception e ) {
	    				printInfo("Suppressed Exception: " + e.getMessage());
	    				if ( ! ( e instanceof SQLException ) ) e.printStackTrace();
	    			}
	    		else
	    			executeSQL( lastSQL );
	    	
	    	if ( showtimes )
	    		printStream.println("Combined totals: Fetched " + rnumRows + " rows. Total Time: " + rttime + "ms (Execution Time: " + retime + "ms)");
		}
    }
    
    
    private void executeSQL( String sql ) throws Exception {//throws CommonException {

    	// c will be null if processSQLs was called directly
    	if ( null == instanceConn || instanceConn.isClosed() ) instanceConn = getConnection();
    	
		long timeNow, etime;
        boolean isNextResultAResultSet = true;

		printInfo( sql );
		
		if ( null != summaryResults ) {
			String sqlu = sql.toUpperCase();
			if ( sqlu.startsWith("CREATE ")) summaryResults[CREATES]++;
	    	else if ( sqlu.startsWith("DROP ")) summaryResults[DROPS]++;
	    	else if ( sqlu.startsWith("INSERT ")) summaryResults[INSERTS]++;
	    	else if ( sqlu.startsWith("DELETE ")) summaryResults[DELETES]++;
	    	else if ( sqlu.startsWith("UPDATE ")) summaryResults[UPDATES]++;
	    	else if ( sqlu.startsWith("SELECT ")) summaryResults[SELECTS]++;
	    	else if ( sqlu.startsWith("CALL ")) summaryResults[CALLS]++;
	    	else summaryResults[OTHERS]++;
		}
		
		PreparedStatement pstmt = null;
		
		int parmsIndex = sql.indexOf("PARMS");
		if ( -1 != parmsIndex ) {
			
			String[] parms = sql.substring( parmsIndex + 6 ).split("[\\s]+");
			sql = sql.substring( 0, parmsIndex );
			
			timeNow = System.currentTimeMillis();
			pstmt = getPreparedStatement(sql);
			
			ParameterMetaData pmd = pstmt.getParameterMetaData();
			
			for (int arrayIndex=0; arrayIndex<parms.length; arrayIndex++) {
				String s = parms[ arrayIndex ];
				int i = arrayIndex+1;
				
				printInfo("Setting statement parameter for parm: " + i + ", value: '" + s + "', JDBC type: " + pmd.getParameterTypeName(i));
				
				switch ( pmd.getParameterType(i) ) {
		            case Types.DECIMAL: case Types.NUMERIC: pstmt.setBigDecimal( i, BigDecimal.valueOf( Long.parseLong(s) ) ); break;
		            case Types.CHAR: case Types.VARCHAR: case Types.LONGVARCHAR: pstmt.setString( i, s ); break;
		            case Types.BINARY: case Types.VARBINARY: case Types.LONGVARBINARY: pstmt.setBytes( i, s.getBytes() ); break;
		            case Types.BIT: case Types.BOOLEAN: pstmt.setBoolean( i, s.equals("true") ); break;
		            case Types.BLOB: pstmt.setObject( i, s.getBytes() ); break; // pstmt.setBlob( i, (Blob) data ); break;
		            case Types.CLOB: pstmt.setObject( i, s.getBytes() ); break; // pstmt.setClob( i, (Clob) data ); break;
		            case Types.DATE: pstmt.setDate( i, Date.valueOf(s) ); break;
		            case Types.TIME: pstmt.setTime( i, Time.valueOf(s) ); break;
		            case Types.TIMESTAMP: pstmt.setTimestamp( i, Timestamp.valueOf(s) ); break;
		            case Types.INTEGER: pstmt.setInt( i, Integer.parseInt(s) ); break;
		            case Types.BIGINT: pstmt.setLong( i, Long.parseLong(s) ); break;
		            case Types.SMALLINT: pstmt.setShort( i, Short.parseShort(s) ); break;
		            case Types.TINYINT: pstmt.setByte( i, Byte.parseByte(s) ); break;
		            case Types.DOUBLE: case Types.FLOAT: pstmt.setDouble( i, Double.parseDouble(s) ); break;
		            case Types.REAL: pstmt.setFloat( i, Float.parseFloat(s) ); break;
	//	            case Types.ARRAY: pstmt.setArray( i, (Array) data ); break;
	//	            case Types.JAVA_OBJECT: case Types.STRUCT: pstmt.setObject( i, data ); break;
	//			    case Types.REF: case Types.BLOB: case Types.CLOB: case Types.ARRAY: pstmt.setObject( i, data ); break;
	//	            case Types.DATALINK: pstmt.setURL( i, (URL) data ); break;
	//	            case Types.REF: pstmt.setRef( i, (Ref) data ); break;
		            case Types.DISTINCT: case Types.NULL: case Types.OTHER: pstmt.setNull( i, Types.NULL ); break; // No distinct type supported
		            default: throw new SQLException("Unsupported JDBC type: " + pmd.getParameterType(i));
				}
			}
			
		} else {
			timeNow = System.currentTimeMillis();
//			CallableStatement cstmt = stmt.getConnection().prepareCall( sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );
						
			pstmt = getPreparedStatement(sql);
		}
		
		isNextResultAResultSet = pstmt.execute();
		etime = System.currentTimeMillis() - timeNow;
//		printInfo( "Executed statement in " + etime + "ms" );
		
		if ( dev ) {
			printStream.println("[press return to get result and fetch rows]> ");
			stdin.readLine();
		}
		
//        Vector resultSets = new Vector();
        
//            while ( false == isNextResultAResultSet ) {
//                isNextResultAResultSet = stmt.getMoreResults();
//            }
//            resultSets.add( stmt.getResultSet() );
        
        while ( true ) {
        	
            if ( true == isNextResultAResultSet ) {
                
            	ResultSet rs = pstmt.getResultSet();

//        		if ( dev ) {
//            		printStream.println("[press return to fetch rows]> ");
//        			stdin.readLine();
//        		}
        		
            	int numRows = processResultSet( rs );
            	
        		if ( dev ) {
            		printStream.println("[press return to close]> ");
        			stdin.readLine();
        		}
            	
            	rs.close();

            	long ttime = System.currentTimeMillis() - timeNow;
            	printInfo("Fetched " + numRows + " rows. Total Time: " + ttime + "ms (Execution Time: " + etime + "ms)");
            	
            	rttime += ttime; retime += etime; rnumRows += numRows;
            	
            	break;

            } else {
            	
            	int updateCount = pstmt.getUpdateCount();

            	if ( -1 == updateCount ) {
//            	    printInfo("\nNo more results (getUpdateCount returned -1)");
                	break;
            	} else {
                	printInfo("Update count: " + updateCount + " (Execution Time: " + etime + "ms)");
            	}
             }
        
             isNextResultAResultSet = pstmt.getMoreResults();
        }
        
        
//        if ( dev ) {
//	        
//	        
//	        ResultSet rs = ((PreparedStatement)stmt).executeQuery();
//	        
//    		if ( dev ) {
//        		printStream.println("[press return to fetch rows]> ");
//    			stdin.readLine();
//    		}
//    		
//        	int numRows = processResultSet( rs );
//        	
//    		if ( dev ) {
//        		printStream.println("[press return to close]> ");
//    			stdin.readLine();
//    		}
//        	
//        	rs.close();
//
//        	long ttime = System.currentTimeMillis() - timeNow;
//        	printInfo("Fetched " + numRows + " rows. Total Time: " + ttime + "ms (Execution Time: " + etime + "ms)");
//        	
//        	rttime += ttime; retime += etime; rnumRows += numRows;
//        }

		SQLWarning nextWarning = pstmt.getWarnings();
		
		Vector<SQLWarning> warnings = new Vector<SQLWarning>();
		
		while ( null != nextWarning ) {
		    warnings.add( nextWarning );
		    nextWarning = nextWarning.getNextWarning();
		}
		
		if ( !CACHE_PREPARED_STATEMENTS )
			pstmt.close();
		
		if ( 0 < warnings.size() ) printInfo("Statement Warnings: " + warnings);
		
		printInfo("");
    }

    private PreparedStatement getPreparedStatement( String sql ) throws SQLException {
    	
    	if ( !CACHE_PREPARED_STATEMENTS ) return instanceConn.prepareStatement(sql);
    	
    	PreparedStatement pstmt = (PreparedStatement) preparedStatements.get(sql);
    	
		if ( null != pstmt ) {
			
			boolean isConnectionInvalid = true;
			try { isConnectionInvalid = pstmt.getConnection().isClosed(); } catch ( SQLException e1 ) {}
			
			if ( isConnectionInvalid ) {
				preparedStatements.clear();
				pstmt = null;
			}
		}
		
		if ( null == pstmt ) {
			// c should not be null
//			if ( null == c || c.isClosed() ) c = getConnection();
			pstmt = instanceConn.prepareStatement( sql );
			preparedStatements.put(sql, pstmt);
		}
		
		return pstmt;
    }    
    
    /**
     * Connect to a database.
     * 
     * @param url the URL of the database
     * @param username the username to use
     * @param password the password for the user
     * @throws SQLException if there was a problem connecting to the database
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Connection sqlConnect() throws SQLException { return instanceConn; }
    
    public Connection getConnection() throws SQLException {
    	if ( null == instanceConn || instanceConn.isClosed() ) {
//    		printStream.println("Driver login timeout1: " + DriverManager.getLoginTimeout());
//    		DriverManager.setLoginTimeout(10); // doesn't make any difference with derby.. but hoping it will work in future releases..
//    		printStream.println("Driver login timeout2: " + DriverManager.getLoginTimeout());
    		ensureConnectionPropertiesAreNowSet();
    		instanceConn = sqlConnect();
//    		try { c = sqlConnect(); }
//    		catch ( SQLException e ) { printStream.println("Unable to obtain connection: " + e); } //e.printStackTrace(); }
    	}
    	if ( null == instanceConn ) throw new SQLException("Unable to obtain connection - null");
    	return instanceConn;
    }
    
    public Statement createStatementOffInternalConnection() throws SQLException {
    	if ( null == instanceConn ) throw new SQLException("SQLDerbyRunner: Connection is not set");
    	return instanceConn.createStatement();
    }
    
    protected void loadDriver( String driver ) {
        try {
            Class.forName( driver ).newInstance();
                                    
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void close() throws SQLException {
    	if ( null != instanceConn ) instanceConn.close();
    }
    
    public class SQLRunnerException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		public SQLRunnerException() { super(); }
    	public SQLRunnerException(String message, Throwable cause) { super(message, cause); }
    	public SQLRunnerException(String message) { super(message); }
    	public SQLRunnerException(Throwable cause) { super(cause); }
    }
}
