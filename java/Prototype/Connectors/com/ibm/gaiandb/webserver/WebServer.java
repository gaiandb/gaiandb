/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.webserver;
/**
 *  @author Patrick Dantressangle
 *  a prototype of a  very simple, multi-threaded HTTP server for Serving GaianDB  pre-canned SQL queries 
 *  This is reusing code as-is from the WebQueryModule, with some modifications as it is not part of a proper servlet server.
 *  for instance, it is using a StringBuffer to return the page instead of using a stream.
 *  But for now, this is good enough  to demonstrated how this function can work minimally ... in 36K jar!! ;-) 
 * 
 **/

import java.io.*;
import java.net.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianNode;


import java.sql.Clob;
import java.sql.Blob;

//TODO: these 3 import are not used for now. They will probably be to consolidate webquerymodule code and wwwserver code.
//import javax.servlet.ServletException;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;

//types of queries: 
// http://localhost:8080/WebQueryModule/resources/queries/LT0byNode/excel?GDB_NODE=Patrick_W150&PARAM2=Value2
// http://localhost:8080/WebQueryModule/resources/queries/LT0byNode?GDB_NODE=Patrick_W150&PARAM2=Value2
// http://localhost:8080/JavaSource/com/ibm/gaiandb/EntityMatrixJoiner.java
// http://localhost:8080/ 


public class WebServer {
	     //GaianDB/Derby specific information
		protected static final String ndriver  = GaianDBConfig.DERBY_CLIENT_DRIVER;
		protected static final String edriver  = GaianDBConfig.DERBY_EMBEDDED_DRIVER;
		protected static final boolean standalone = false;
		protected static final String DEFAULT_USR = GaianDBConfig.GAIAN_NODE_DEFAULT_USR;
		protected static final String DEFAULT_PWD = GaianDBConfig.GAIAN_NODE_DEFAULT_PWD;
		protected static final String DEFAULT_HOST = "localhost";
		protected static final int DEFAULT_PORT = GaianNode.DEFAULT_PORT;
		protected static final String DEFAULT_DATABASE = GaianDBConfig.GAIANDB_NAME;
	
		protected static final String DBMS   = "derby";
	    
		//TODO:WQM specific information ;may need to go away when refactoring of WQM  is done
		protected static final String WQM_QueryPrefix="WebQueryModule/resources/queries/";
		protected static final String QUERY_TABLE = "GDB_Local_Queries";
		protected static final String QUERY_FIELDS_TABLE = "GDB_Local_Query_Fields";
		protected static final String RESPONSE_FORMAT_BLOB = "blob";
		protected static final String RESPONSE_FORMAT_BLOB_GZIPPED = "blobgzipped";
		protected static final String RESPONSE_FORMAT_CLOB = "clob";
		protected static enum ResponseFormat { JSON, EXCEL, BLOB, BLOB_GZIPPED, CLOB };
		
		
	   /* 2XX: OK codes */
	   public static final int HTTP_OK = 200;
	   public static final int HTTP_CREATED = 201;
	   public static final int HTTP_ACCEPTED = 202;
	   public static final int HTTP_NOT_AUTHORITATIVE = 203;
	   public static final int HTTP_NO_CONTENT = 204;
	   public static final int HTTP_RESET = 205;
	   public static final int HTTP_PARTIAL = 206;

	   /* 3XX: relocation or redirect */
	   public static final int HTTP_MULT_CHOICE = 300;
	   public static final int HTTP_MOVED_PERM = 301;
	   public static final int HTTP_MOVED_TEMP = 302;
	   public static final int HTTP_SEE_OTHER = 303;
	   public static final int HTTP_NOT_MODIFIED = 304;
	   public static final int HTTP_USE_PROXY = 305;

	   /* 4XX: client error */
	   public static final int HTTP_BAD_REQUEST = 400;
	   public static final int HTTP_UNAUTHORIZED = 401;
	   public static final int HTTP_PAYMENT_REQUIRED = 402;
	   public static final int HTTP_FORBIDDEN = 403;
	   public static final int HTTP_NOT_FOUND = 404;
	   public static final int HTTP_BAD_METHOD = 405;
	   public static final int HTTP_NOT_ACCEPTABLE = 406;
	   public static final int HTTP_PROXY_AUTH = 407;
	   public static final int HTTP_CLIENT_TIMEOUT = 408;
	   public static final int HTTP_CONFLICT = 409;
	   public static final int HTTP_GONE = 410;
	   public static final int HTTP_LENGTH_REQUIRED = 411;
	   public static final int HTTP_PRECON_FAILED = 412;
	   public static final int HTTP_ENTITY_TOO_LARGE = 413;
	   public static final int HTTP_REQ_TOO_LONG = 414;
	   public static final int HTTP_UNSUPPORTED_TYPE = 415;

	   /* 5XX: server error */
	   public static final int HTTP_SERVER_ERROR = 500;
	   public static final int HTTP_INTERNAL_ERROR = 501;
	   public static final int HTTP_BAD_GATEWAY = 502;
	   public static final int HTTP_UNAVAILABLE = 503;
	   public static final int HTTP_GATEWAY_TIMEOUT = 504;
	   public static final int HTTP_VERSION = 505;
	   
 

/* static class data/methods */
   static boolean debug = false;
   
   /* console print  */
   protected static void console_print(String s) {
	   if (debug)
		   System.out.println(s);
   }

   /* print to the log file */
   protected static void log(String s) {
	   if (debug){
		   synchronized (log) {
			   log.println(s);
			   log.flush();
		   }
	   }
   }

   static PrintStream  log = null;
   /* our server's configuration information is stored
    * in these properties
    */
   protected static Properties ConfigurationProperties = new Properties();

   /* Where worker threads stand idle */
   static Vector<PageWorker> threads = new Vector<PageWorker>();

   /* the web server's virtual root */
   static File root;
   static boolean directory_browsing;
   
   /* timeout on client connections */
   static int timeout = 0;

   /* max # worker threads */
   static int maxpageWorkers = 5;
   /* used when shutting down the server*/
   protected boolean running =false;
   /* default port */
   private static int port = 8080;
   
   // used for HTML end of lines
   static final String EOL ="\r\n"; 
   //{(byte)'\r', (byte)'\n' };


   /***************************************************************
    *  load www-server.properties from java.home 
    *  just basic setting for now.
    **************************************************************/
   static void loadConfiguration() throws IOException {
       //File f = new File(System.getProperty("java.home")+File.separator+"lib"+File.separator+"GDBWWW.properties");
	   File f=new File(System.getProperty("user.dir")+ File.separator+"GDBWWW.properties");
	   //System.out.println("Taking default values as GDB WWW configuration file does not exist: ["+f.getAbsolutePath()+"]");
       if (f.exists()) {
           InputStream is =new BufferedInputStream(new FileInputStream(f));
           ConfigurationProperties.load(is);
           is.close();
           String r = ConfigurationProperties.getProperty("root");
           if (r != null) {
               root = new File(r);
               if (!root.exists()) {
                   //throw new Error
                   System.out.println(root + " doesn't exist as server root. Taking defaults for root");
                   root=null;
               }
           }
           r = ConfigurationProperties.getProperty("directory_browsing");
           if (r != null){
        	   directory_browsing=(r.trim().equalsIgnoreCase("true"));
           }
           r = ConfigurationProperties.getProperty("debug");
           if (r != null){
        	   debug=(r.trim().equalsIgnoreCase("true"));
           }
           r = ConfigurationProperties.getProperty("timeout");
           if (r != null) {
               timeout = Integer.parseInt(r.trim());
           }
           r = ConfigurationProperties.getProperty("port");
           if (r != null) {
               port = Integer.parseInt(r.trim());
           }
           r = ConfigurationProperties.getProperty("workers");
           if (r != null) {
               maxpageWorkers = Integer.parseInt(r);
           }
           r = ConfigurationProperties.getProperty("log");
           if (r != null) {
        	   r=r.replaceAll("//", File.separator);
               console_print("opening log file: " + r);
               try{
            	   log = new PrintStream(new BufferedOutputStream(  new FileOutputStream(r)));
               }
               catch (FileNotFoundException e){
            	   System.out.println("The system cannot find the path specified["+r+"].revert to default behavior");
            	   log=null;
               }
           }
       }
       else
    	   System.out.println("Taking default values as GDB WWW configuration file does not exist: ["+f.getAbsolutePath()+"]");

       /* if no properties were specified, choose defaults */
       if (root == null) {
           root = new File(System.getProperty("user.dir"));
       }
       if (timeout <= 1000) {
           timeout = 5000;
       }
       /* put a minimum amount of workers..*/
       if (maxpageWorkers < 5) {
           maxpageWorkers = 5;
       }
       if (log == null) {
           console_print("logging to stdout");
           log = System.out;
       }
   }
   /**
    * just basic dump  of configuration if required.
    */
   static void printconfiguration() {
       console_print("root="+root);
       console_print("timeout="+timeout);
       console_print("workers="+maxpageWorkers);
   }

   /**
    * used to kick start it from the command line
    * @param a
    * @throws Exception
    */
   public static void main(String[] a) throws Exception {
       if (a.length > 0) {
           port = Integer.parseInt(a[0]);
       }
       loadConfiguration();
       printconfiguration();
       /* start worker threads */
       for (int i = 0; i < maxpageWorkers; ++i) {
           PageWorker w = new PageWorker();
           (new Thread(w, "worker #"+i)).start();
           threads.addElement(w);
       }

       ServerSocket ss = new ServerSocket(port);
	   System.out.println("GDBWWW server started on port "+port);

       while (true) {
           Socket s = ss.accept();

           PageWorker w = null;
           synchronized (threads) {
               if (threads.isEmpty()) {
                   PageWorker ws = new PageWorker();
                   ws.setSocket(s);
                   (new Thread(ws, "additional worker")).start();
               } else {
                   w = threads.elementAt(0);
                   threads.removeElementAt(0);
                   w.setSocket(s);
               }
           }
       }
   }
}


/*********************************************************
 * This worker is in charge of serving the page and all GDB queries
 * @author dantress
 *
 **********************************************************/
class PageWorker extends WebServer implements  Runnable {
   final static int BUFFER_SIZE = 2048;

   /* buffer to use for requests */
   byte[] buffer;
   /* Socket to client we're handling */
   private Socket remoteSocket=null;
   /* have one SQL connection per worker and reuse it */
   static Connection conn=null;

   PageWorker() {
       buffer = new byte[BUFFER_SIZE];
   }
   
   PageWorker(Socket rs) throws Exception  {
       buffer = new byte[BUFFER_SIZE];
       if (rs !=null)
    	   remoteSocket = rs;
   }

   synchronized void setSocket(Socket s) {
       this.remoteSocket = s;
       notify();
   }

   public synchronized void run() {
       while(true) {
           if (remoteSocket == null) {
               try {
                   wait();
               } catch (InterruptedException e) {
                   /* should not happen */
                   continue;
               }
           }
           try {
               handleClient();
           } catch (Exception e) {
               e.printStackTrace();
           }
           /* go back in wait queue if there's fewer than numHandler connections.*/
           remoteSocket = null;
           Vector<PageWorker> threadPool = WebServer.threads;
           synchronized (threadPool) {
               if (threadPool.size() >= WebServer.maxpageWorkers) {
                   /* too many threads, exit this one */
                   return;
               } else {
                   threadPool.addElement(this);
               }
           }//endofsynchronized code
       }//endofwhile
   }
   /*********************************************************
    * Handle HTML client request: Mainly GET and HEAD for now. POST for later. 
    * @throws IOException
    *********************************************************/
   void handleClient() throws IOException {
       BufferedReader in = new BufferedReader(new InputStreamReader( remoteSocket.getInputStream()));
       PrintStream    ps = new PrintStream(remoteSocket.getOutputStream());
       /* we will only block in read for this many milliseconds
        * before we fail with java.io.InterruptedIOException,
        * at which point we will abandon the connection.
        */
       remoteSocket.setSoTimeout(WebServer.timeout);
       remoteSocket.setTcpNoDelay(true);
       try {
           // Read data from client - we only really care about the first line, but read it all
           String currentLine = in.readLine();
           String headerLine = currentLine;
           if (headerLine==null)
        	   return;
           StringTokenizer tokenizer = new StringTokenizer(headerLine);
           String HttpMethod=tokenizer.nextToken();
           /* is this request a GET/POST or just a HEAD? */
           boolean doingGet;
           if ( HttpMethod.equals("GET")){
               doingGet = true;
           } 
           else if ( HttpMethod.equals("HEAD")){
               doingGet = false;
           } else {
               /* we don't support this method */
               ps.println("HTTP/1.0 " + HTTP_BAD_METHOD +" unsupported method type: "+HttpMethod);
               ps.flush();
               remoteSocket.close();
               return;
           }

           /* find the file name, from: GET /xxx/mypage.html HTTP/1.0
            * extract "/xxx/mypage.html"
            */
            String fname = tokenizer.nextToken();
        	  
           if (fname.startsWith("/")) {
               fname = fname.substring(1);
           }
           //urls of style :  http://localhost:8080/WebQueryModule/...
           if (doingGet)
           {
        	   processHTTPGET(ps, doingGet,fname);
           }

       } finally {
           remoteSocket.close();
       }
   }

   /**********************************************************************
    * Process a GDB specific GET METHOD  (mainly queries and other pre-canned things ) 
    * @param ps
    * @param doingGet
    * @param fname
    * @throws IOException
    *********************************************************************/
   private void processHTTPGET(PrintStream ps, boolean doingGet,String fname) throws IOException {
	   if(fname.toLowerCase().contains("dashboard/"))
	   {
		   processDashboardQuery(ps, doingGet,fname);
	   }
	   else
	   if(fname.startsWith("WebQueryModule/"))
	   { //process specific GaianDB queries        	           	   
	      processGDBQuery(ps, doingGet,fname); 
	   }
	   else { //process basic  files/pages...
		   File targ = new File(WebServer.root, fname);
		   if (targ.isDirectory()) {
			   File ind = new File(targ, "index.html");
			   if (ind.exists()) {
				   targ = ind;
			   }
		   }
		   boolean OK = printHeaders(null,targ, ps);
		   if (doingGet) {
			   if (OK) {
				   sendFile(targ, ps);
			   } else {
				   sendHTTPError(404,targ.getAbsolutePath(), ps);
			   }
		   }
	   }
   }
   
   /************************************************
    * process pre-canned dashboard queries..instead of using the JAVA application.
    * Here is the mapping of URLS..
    * LISTQUERIES: /Dashboard/ListLocalQueries
    * 
    * @param ps
    * @param doingGet
    * @param fname
    ***********************************************/
   private void processDashboardQuery(PrintStream out, boolean doingGet, String request) 
   {
	   try{
		   if (conn ==null)	  
			   conn = getDbConnection(DEFAULT_DATABASE,DEFAULT_HOST,DEFAULT_PORT,DEFAULT_USR, DEFAULT_PWD);
			Statement statement = conn.createStatement();
			try {
			  if(request.toLowerCase().endsWith("dashboard/listqueries"))
				executeQueryFromQueryString("CALL LISTQUERIES(0)", null, ResponseFormat.EXCEL); 
			  else {
				  System.out.println("argh..."+request);
				  sendHTTPError(404,request, out);
			  }
			} finally {
				statement.close();
			}
	   }
	   //humm... couldn't create the query...send 404 error... 
	   catch (SQLException e) {
		   System.err.println("GaianDB WebServer: Unable to access a running GaianDB ");
		   e.printStackTrace();
		   sendHTTPError(500,request, out);
	   }
	   catch (Exception e) {
		   System.err.println("GaianDB WebServer: Unable to process request/query ");
		   e.printStackTrace();
		   sendHTTPError(500,request, out);
	   }
   }

/***************************************************
    * Load the derby driver.
    * @param driver
    *************************************************/
   private static boolean driverloaded=false;
   protected void loadDriver( String driver ) {
	   try {
		   if (driverloaded==false)
		   {
			   Class.forName( driver ).newInstance();
			   driverloaded=true;
		   }
	   } catch (Exception e) {
		   e.printStackTrace();
	   }
   }
   
   /*************************************************
    * Get a GaianDB /Derby connection. (taken from Derbyrunner ) 
    *************************************************/
   private Connection getDbConnection(String mDatabase, String mHost, int mPort, String mUser, String mPasswd) throws SQLException {
	   if ((conn ==null) || (conn.isClosed())) 
	   {
		   String url;
		   if ( standalone  ) {
			   loadDriver( edriver );
			   url = "jdbc:" + DBMS + ":" + mDatabase + ";"; //create=" + createdb + (upgrade?";upgrade=true":"");

		   } else {
			   loadDriver( ndriver );
			   url = "jdbc:" + DBMS + "://" + mHost + ":" + mPort + "/" + mDatabase + ";"; //create=" + createdb + (upgrade?";upgrade=true":"");
		   }
		   conn= DriverManager.getConnection( url, mUser, mPasswd );
	   }
	  return conn;
   }

   /**********************************************************
    * TODO:Taken directly from WQM : list queries.... need to be refactored out
    *********************************************************/
   public void listQueries(StringBuffer out, ResponseFormat RetFormat) throws SQLException, IOException {	    
		try {			
			Statement statement = conn.createStatement();
			try {
				ResultSet results = statement.executeQuery("SELECT id, description,issuer,last_extracted,query FROM " + QUERY_TABLE);
				try {
					if (RetFormat == ResponseFormat.JSON ) 
						 out.append('[');
					else out.append("<HTML><TITLE>List of queries</TITLE><BODY><H1>List of queries</H1><TABLE BORDER=1><TR><TH>Id<TH>Description<TH>Issuer<TH>Last Extracted</TR>");
					boolean first = true;
					while (results.next()) {
						if (first) {
							first = false;
						} else {
							if (RetFormat == ResponseFormat.JSON ) 
								out.append(",\n");
							//else out.append("</TR><TR>");
						}
						if (RetFormat == ResponseFormat.JSON )
							{
							out.append("{\"id\":\"" + results.getString("id") + "\",");
							out.append("\"description\":\"" + results.getString("description") + "\"}");
							out.append("\"issuer\":\"" + results.getString("issuer") + "\"}");
							out.append("\"last_extracted\":\"" + results.getString("last_extracted") + "\"}");							
							}
						else
						{
							String Qid=results.getString("id");
							out.append("<TR><TD><A HREF=\"/"+WQM_QueryPrefix+Qid+"/excel\">"+Qid+"</A></TD>"+
									   "<TD>"+results.getString("description")+"</TD>"+
									   "<TD>"+results.getString("issuer")+"</TD>"+
									   "<TD>"+results.getString("last_extracted")+"</TD>"+
									   "</TR>"+EOL);
						}
					}
					if (RetFormat == ResponseFormat.JSON )  
						 out.append(']');
					else out.append("</TABLE></BODY></HTML>");
				} finally {
					results.close();
				}
			} finally {
				statement.close();
			}
		} finally {
			//don't close in WWGDBserver so it can be reused...
			//TODO:WQM: conn.close();
		}
	}

   /********************************************************8
    * Get the Query parameters....
    * @param query
    * @return
    ******************************************************/
   public static Map<String, String> getQueryParameters(String query)  
   {  
	   Map<String, String> map = new HashMap<String, String>();  
	   if (query !=null){
		   String[] params = query.split("&");  
		   for (String param : params)  
		   {  
			   String name = param.split("=")[0];  
			   String value = param.split("=")[1];  
			   map.put(name, value);  
		   }  
	   }
	   return map;  
   } 
   
//   protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
//	   request.
//   }
   
   /***********************************************************************
    * Process GDB WQM queries... try to  mimic  WQM Servlet module here..obviously incomplete.
    * @param ps
    * @param request
    *********************************************************************/
   private void processGDBQuery(PrintStream ps, boolean doingGet,String request)  {

	   String[] parmsStr = request.split("\\?");	   
	   String[] parms = parmsStr[0].split("/");
	   
	  // @SuppressWarnings("unchecked")
	   Map<String, String> queryParams =null;
	   if (parmsStr.length>1)
	      queryParams = getQueryParameters(parmsStr[1]);  //in WQM: .request.getParameterMap();
	   String queryId =parms[3];
	   ResponseFormat responseFormat = ResponseFormat.JSON;
	   //extract query name.
	   try{
		   if ((conn ==null)|| (conn.isClosed()))
			   conn = getDbConnection(DEFAULT_DATABASE,DEFAULT_HOST,DEFAULT_PORT,DEFAULT_USR, DEFAULT_PWD);
		   StringBuffer Query_output=new StringBuffer("");//<HTML><BODY><TITLE>Found tokens</TITLE><H1>Found the following tokens</H1><PRE>");
		   if (parms.length>=5){
			   if (parms[4].equalsIgnoreCase("excel"))
					   responseFormat=ResponseFormat.EXCEL;
		   }		      

		   if (request.startsWith(WQM_QueryPrefix+"ListQueries"))
		   {
			   listQueries(Query_output,responseFormat);
		   }
		   else {
			   Query_output.append( executeQueryFromQueryID( queryId, queryParams, responseFormat));
//for debugging purposes only...
//			   Query_output.append("request url:"+request+EOL);
//			   for (String p: parms) {
//				   Query_output.append("Found token:"+p+EOL);
//			   };
//			   Query_output.append("Not recognized query or command"+EOL);
		   }
		   //Query_output.append("</PRE></BODY></HTML>");
		   boolean OK = printHeaders(Query_output,null, ps);
		   if (doingGet){
			   if (OK) {
				  if (debug)
					  console_print(Query_output.toString());
				   ps.println(Query_output);
				   ps.flush();
			   } else {
				   sendHTTPError(404,request, ps);
			   }
		   }
	   }
	 //humm... couldn't create the query...send 404 error... 
	   catch (SQLException e) {		   
		   if (debug){
			   System.err.println("GaianDB WebServer: Unable to access a running GaianDB ");
			   e.printStackTrace();
		   }
		   sendHTTPError(500,request, ps);
	   }
	   catch (Exception e) {
		   if (debug){ 
			   System.err.println("GaianDB WebServer: Unable to process request/query ");
			   e.printStackTrace();
		   }
		   sendHTTPError(500,request, ps);
	   }
   }
	
   /***************************************************************************************
    *TODO: WQM:  should be factored out when WQM code is refactored independant of Servlet,
    *  
    *************************************************************************************/
    private String getRequestFieldValue(String field, Map<String, String> queryParams) throws Exception {
    
		String fieldValues = queryParams.get(field); //WQM removed [] 
		if (fieldValues == null) {
			throw new Exception("Missing parameter to query: \"" + field + "\".");
		} 
		//WQM: else if (fieldValues.length != 1) {
		//WQM:	throw new  Exception("Multiple values provided for parameter \"" + field + "\".");
		//WQM:}
		return fieldValues; //WQM[0];
	}
    /*****************************************************************************************8
     * this method is for executing Queries directly (not using the table of queries)
     * mainly  to have calls to SQL procedures directly mapped to URLs 
     * @param queryTemplate
     * @param queryParams
     * @param responseFormat
     * @return
     * @throws Exception
     **************************************************************************************/
    public StringBuffer executeQueryFromQueryString( String queryTemplate, Map<String, String> queryParams, ResponseFormat responseFormat) throws  Exception {
		log("Executing query '" + queryTemplate + "'");
		Connection conn = getDbConnection(DEFAULT_DATABASE,DEFAULT_HOST,DEFAULT_PORT,DEFAULT_USR, DEFAULT_PWD);
		StringBuffer response=new StringBuffer("");
		//ResponseFormat responseFormatString=ResponseFormat.EXCEL;
		PreparedStatement statement= conn.prepareStatement(queryTemplate);
		try {
			//not used for now: ParameterMetaData pmd = statement.getParameterMetaData();
			//not useful  for now... : int numParams = pmd.getParameterCount();			
			boolean returnedResultSet = statement.execute();
			ResultWriter rw = new HtmlResultWriter(response);
			if (returnedResultSet) {
				ResultSet results = statement.getResultSet();
				try {
					ResultSetMetaData rsmd = results.getMetaData();
					
					int colCount = rsmd.getColumnCount();
					String[] cols = new String[colCount];
					for (int i = 0; i < colCount; i++) {
						cols[i] = rsmd.getColumnLabel(i + 1);
					}
					
					rw.writeHeader(cols);
					
					boolean first = true;
					while (results.next()) {
						if (first) {
							first = false;
						} else {
							rw.writeRowSeparator();
						}
						Object[] values = new Object[colCount];
						for (int i = 0; i < colCount; i++) {
							values[i] = results.getObject(i + 1);
						}
						rw.writeRow(values);
					}
					rw.writeFooter();
				} finally {
					results.close();
				}
			} else {
				rw.writeUpdateCount(statement.getUpdateCount());
			}
		} finally {
			statement.close();
		}
		return response;		
    }

    /***************************************************************************************
     * TODO:WQM:  should be factored out when WQM code is refactored independent of Servlet,
     *  
     *************************************************************************************/
    public StringBuffer executeQueryFromQueryID( String queryId, Map<String, String> queryParams, ResponseFormat responseFormat) throws  Exception {
		log("Executing query '" + queryId + "'");
		Connection conn = getDbConnection(DEFAULT_DATABASE,DEFAULT_HOST,DEFAULT_PORT,DEFAULT_USR, DEFAULT_PWD);
		StringBuffer response=new StringBuffer("");
		try {
			// Get query template from database
			String queryTemplate=null, responseFormatString=null;
			PreparedStatement statement = conn.prepareStatement("SELECT query, response_format FROM " + QUERY_TABLE + " WHERE id = ?");
			try {
				statement.setString(1, queryId);
				ResultSet results = statement.executeQuery();
				try {
					if (results.next()) {
						queryTemplate = results.getString("query");
						responseFormatString = results.getString("response_format");
					} 
// TODO:           Should be processed above in GDBWW server , sending back either a 404 or 500 HTTP error.
					else {
						throw new SQLException("Could not find query \"" + queryId + "\"");
					}
				} finally {
					results.close();
				}
			} finally {
				statement.close();
			}
			
			// Both JSON and EXCEL response formats will be overridden by BLOB/CLOB
			if (responseFormatString != null) {
				if (responseFormatString.equals(RESPONSE_FORMAT_BLOB)) {
					responseFormat = ResponseFormat.BLOB;
				} else if (responseFormatString.equals(RESPONSE_FORMAT_BLOB_GZIPPED)) {
					responseFormat = ResponseFormat.BLOB_GZIPPED;
				} else if (responseFormatString.equals(RESPONSE_FORMAT_CLOB)) {
					responseFormat = ResponseFormat.CLOB;
				}
			}
			
			// Get fields from database
			List<String> fields = new ArrayList<String>();
			SortedMap<Short, String> substitutionFields = new TreeMap<Short, String>();
			statement = conn.prepareStatement("SELECT name, seq, offset FROM " + QUERY_FIELDS_TABLE + " WHERE query_id = ? ORDER BY seq, offset");
			try {
				statement.setString(1, queryId);
				ResultSet results = statement.executeQuery();
				try {
					while (results.next()) {
						if (results.getObject("seq") != null) {
							fields.add(results.getString("name"));
						} else {
							substitutionFields.put(results.getShort("offset"), results.getString("name"));
						}
					}
				} finally {
					results.close();
				}
			} finally {
				statement.close();
			}
			
			// Execute query with passed parameters
			
			// Do query variable substitution
			int fieldLengthDelta = 0;
			if (queryParams != null) {
				for ( Map.Entry<Short, String> f : substitutionFields.entrySet() ) {
					String fieldValue = getRequestFieldValue(f.getValue(), queryParams);
					
					int actualOffset = f.getKey() + fieldLengthDelta;
					
					//String sanitisedValue = sanitiseField(fieldValue, queryTemplate, actualOffset);
					String sanitisedValue = fieldValue;
					queryTemplate = queryTemplate.substring(0, actualOffset) + sanitisedValue + queryTemplate.substring(actualOffset);
					fieldLengthDelta += sanitisedValue.length();
				}
			}
			
			log("Preparing query template with substituted params: " + queryTemplate);
			
			statement = conn.prepareStatement(queryTemplate);
			try {
				ParameterMetaData pmd = statement.getParameterMetaData();
				int numParams = pmd.getParameterCount();
				
				if(fields.size() != numParams) {
					throw new Exception("Invalid query: number of field names does not match number of parameters");
				}
				if(queryParams != null)
				{
					for (int i = 0; i < fields.size(); i++) {
						statement.setObject(i + 1, getRequestFieldValue(fields.get(i), queryParams));
					}
				}
				boolean returnedResultSet = statement.execute();
				
				ResultWriter rw = null;
				switch (responseFormat) {
				case EXCEL:
					rw = new HtmlResultWriter(response); //TODO: WQM.getWriter());
					break;
				case JSON:
					rw = new JsonResultWriter(response); //TODO:.getWriter());
					break;
				case BLOB:
					rw = new BlobResultWriter(response);
					break;
			/*** WQM: not supporting this for now in GDB WWW Server
				case BLOB_GZIPPED:
					rw = new GzippedBlobResultWriter(request, response);
					break;
				case CLOB:
					rw = new ClobResultWriter(response);
					break;
			 ***/
				}
				
				if (returnedResultSet) {
					ResultSet results = statement.getResultSet();
					try {
						ResultSetMetaData rsmd = results.getMetaData();
						
						int colCount = rsmd.getColumnCount();
						String[] cols = new String[colCount];
						for (int i = 0; i < colCount; i++) {
							cols[i] = rsmd.getColumnLabel(i + 1);
						}
						
						rw.writeHeader(cols);
						
						boolean first = true;
						while (results.next()) {
							if (first) {
								first = false;
							} else {
								rw.writeRowSeparator();
							}
							Object[] values = new Object[colCount];
							for (int i = 0; i < colCount; i++) {
								values[i] = results.getObject(i + 1);
							}
							rw.writeRow(values);
						}
						rw.writeFooter();
					} finally {
						results.close();
					}
				} else {
					rw.writeUpdateCount(statement.getUpdateCount());
				}
			} finally {
				statement.close();
			}
		} finally {
			//conn.close();
		}
		return response;
	}
   
   
   /*************************************************************
    *  Print the proper header from scratch. doesn't try  to mimic servlet headers
    * 
    ***************************************************************/
   boolean printHeaders(StringBuffer queryOutput, File targ, PrintStream ps) throws IOException {
       boolean ret = false;
       int rCode = 0;
       String hostaddress=remoteSocket.getInetAddress().getHostAddress();
       if (null != targ && !targ.exists()) {
           rCode = HTTP_NOT_FOUND;
           ps.println("HTTP/1.1 " + HTTP_NOT_FOUND + " not found");
           ret = false;
           log("Page request from " +hostaddress+": GET " + targ.getAbsolutePath()+"-->"+rCode);
       }  else {
           rCode = HTTP_OK;
           ps.println("HTTP/1.1 " + HTTP_OK+" OK");
           ret = true;    
           if (targ ==null && queryOutput != null)
        	   log("Page request from " +hostaddress+": GET "+queryOutput+"-->"+rCode);
           else
               log("Page request from " +hostaddress+": GET " +targ.getAbsolutePath()+"-->"+rCode);
       }
       ps.println("Server: GaianDB WWW server");
       ps.println("Date: " + (new Date()));
       if ((ret) ) {
    	   if (queryOutput != null)
    	   {
    		   int ql=queryOutput.length()+1;
    		   ps.println("Content-Length: "+ql);
               ps.println("Last Modified: " + (new Date()));
       		   ps.println("Cache-Control: no-cache");
       		   ps.println("Pragma: no-cache");
               ps.println("Content-Type: text/html");
    	   }
    	   else
           if ( (null != targ) && !targ.isDirectory()) {
               ps.println("Content-Length: "+targ.length());
               ps.println("Last Modified: " + (new Date(targ.lastModified())));
               String name = targ.getName();
               int ind = name.lastIndexOf('.');
               String ct = null;
               if (ind > 0) {
                   ct = map.get(name.substring(ind));
               }
               if (ct == null) {
                   ct = "unknown/unknown";
               }
               ps.println("Content-Type: " + ct);          
           } else {
               ps.println("Content-Type: text/html");             
           }
    	  // Blank line indicates end of headers
           ps.println();               
       }
       return ret;
   }



    /*****************************************************************
     * return a HTTP   error from trying to fetch a file back
     * @param targ
     * @param ps
     * @throws IOException
     *****************************************************************/
   void sendHTTPError(int HTTPerror, String  reason, PrintStream ps)  {
       try {
		ps.println();
		   ps.println();
		   switch (HTTPerror){
		       case 400: ps.println("The request ("+reason+") cannot be fulfilled due to bad syntax"); break;
		       case 404: ps.println("The requested resource ("+reason+") could not be found but may be available again in the future."); break;
		       case 405: ps.println("A request was made of a page using a request method not supported by that page"); break;
		       case 500: ps.println("Server error\n"+ "The Server failed to  process the query:"+reason+"."); break;
		       default : ps.println("An undertermined error ("+HTTPerror+") occured with reason:"+reason);
		   }
	} catch (Exception e) {
		e.printStackTrace();
	}
   }
   
   
   /*****************************************************************
    * Send a file back to client. 
    * @param targ
    * @param ps
    * @throws IOException
    *****************************************************************/
   void sendFile(File targ, PrintStream out) throws IOException {
       InputStream in = null;
       out.println();
       if (directory_browsing ==false)
    	   return;
       
       if (targ.isDirectory()) {
           listDirectory(targ, out);
           return;
       } else {
           in = new BufferedInputStream(new FileInputStream(targ.getAbsolutePath()));
       }

       try {
    	   byte[] buffer = new byte[BUFFER_SIZE];
    	   int byteCount;
    	   while ( (byteCount = in.read(buffer)) != -1 ) {
    			 out.write(buffer, 0, byteCount);
           out.flush();
           }
       } finally {
           in.close();
       }
   }

   /*****************************************************************
    * mapping of file extensions to content-types
    * @param targ
    * @param ps
    * @throws IOException
    *****************************************************************/
   static java.util.Hashtable<String, String> map = new java.util.Hashtable<String, String>();

   static {
       fillMap();
   }
   static void setHttpApplicationType(String k, String v) {
       map.put(k, v);
   }

   static void fillMap() {
       setHttpApplicationType("", "content/unknown");
       setHttpApplicationType(".uu", "application/octet-stream");
       setHttpApplicationType(".exe", "application/octet-stream");
       setHttpApplicationType(".ps", "application/postscript");
       setHttpApplicationType(".zip", "application/zip");
       setHttpApplicationType(".sh", "application/x-shar");
       setHttpApplicationType(".tar", "application/x-tar");
       setHttpApplicationType(".snd", "audio/basic");
       setHttpApplicationType(".au", "audio/basic");
       setHttpApplicationType(".wav", "audio/x-wav");
       setHttpApplicationType(".gif", "image/gif");
       setHttpApplicationType(".jpg", "image/jpeg");
       setHttpApplicationType(".jpeg", "image/jpeg");
       setHttpApplicationType(".htm", "text/html");
       setHttpApplicationType(".html", "text/html");
       setHttpApplicationType(".text", "text/plain");
       setHttpApplicationType(".c", "text/plain");
       setHttpApplicationType(".cc", "text/plain");
       setHttpApplicationType(".c++", "text/plain");
       setHttpApplicationType(".h", "text/plain");
       setHttpApplicationType(".pl", "text/plain");
       setHttpApplicationType(".txt", "text/plain");
       setHttpApplicationType(".java", "text/plain");
   }

   /***********************************************************************
    * List a directory in HTML with HREF for files
    * @param dir
    * @param ps
    * @throws IOException
    *********************************************************************/
   void listDirectory(File dir, PrintStream ps) throws IOException {
       ps.println("<TITLE>Directory listing</TITLE><P>\n");
       ps.println("<A HREF=\"..\">Parent Directory</A><BR>\n");
       String[] list = dir.list();
       for (int i = 0; list != null && i < list.length; i++) {
           File f = new File(dir, list[i]);
           if (f.isDirectory()) {
               ps.println("<A HREF=\""+list[i]+"/\">"+list[i]+"/</A><BR>");
           } else {
               ps.println("<A HREF=\""+list[i]+"\">"+list[i]+"</A><BR>");
           }
       }
       ps.println("<P><HR><BR><I>" + (new Date()) + "</I>");
   }

   /**********************************************************************
    * shutdown the server on user command..
    *********************************************************************/
   public void shutdown() {
	   System.err.println("GaianDB WebServer: Attemping to shutdown...");
	   running = false;
	   try {
		   remoteSocket.close();
	   } catch (IOException e) {
		   System.err.println("GaianDB WebServer: Unable to close server socket (?!)");
		   e.printStackTrace();
	   }
   }

}



/****************************************************************************
 * TODO: Taken from WQM... would need to be factored out when WQM is rewritten more generically. 
 * @author Dominic Harries
 ***************************************************************************/
interface ResultWriter {
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";

	public void writeHeader(String[] cols) throws IOException, Exception;
	public void writeFooter() throws IOException;
	public void writeRow(Object[] values) throws IOException, Exception;
	public void writeRowSeparator() throws IOException, Exception;
	public void writeUpdateCount(int updateCount) throws IOException, Exception;
}


/****************************************************************************
 * TODO:Taken from WQM... would need to be factored out when WQM is rewritten more generically. 
 * @author Dominic Harries
 **************************************************************************/
 class HtmlResultWriter implements ResultWriter {
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private StringBuffer out;
	
	public HtmlResultWriter(StringBuffer out) {
		this.out = out;
	}
	
	public void writeHeader(String[] cols) throws IOException {
		out.append("<html><head><title>Query Results</title></head><body><table border=\"1\"><tr>");
		for (String col: cols) {
			out.append("<th>");
			out.append(col);
			out.append("</th>");
		}
		out.append("</tr>");
	}

	public void writeFooter() throws IOException {
		out.append("</table></body>");
	}

	public void writeRow(Object[] values) throws IOException {
		out.append("<tr>");
		for (Object value : values) {
			out.append("<td>" + value + "</td>");
		}
		out.append("</tr>");
	}

	public void writeRowSeparator() {
		// Not required
	}

	public void writeUpdateCount(int updateCount) throws IOException {
		out.append(String.valueOf(updateCount));
	}
}

 
 /*****************************************************************************
 * TODO:Taken from WQM... would need to be factored out when WQM is rewritten more generically. 
 * @author Dominic Harries
  ****************************************************************************/
  class JsonResultWriter implements ResultWriter {
// 	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
 	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
 	 	
 	StringBuffer out; //WQM TODO: private Writer out;
 	private String[] cols;
 	
 	public JsonResultWriter(StringBuffer response) {
 		this.out = response;
 	}

 	public void writeHeader(String[] cols) throws IOException {
 		this.cols = cols;
 		out.append("{\"columns\": ["); //TODO:
 		for (int i = 0; i < cols.length; i++) {
 			out.append("\"" + cols[i] + "\""); //TODO:
 			if (i < cols.length - 1) out.append(','); //TODO:
 		}
 		out.append("],\n"); //TODO:		
 		out.append("\"items\": [\n"); //TODO:
 	}

 	public void writeFooter() throws IOException {
 		out.append("]}");//TODO:
 	}

 	public void writeRow(Object[] values) throws IOException {
 		out.append('{');//TODO:
 		for (int i = 0; i < cols.length; i++) {
 			out.append("\"" + cols[i] + "\":");//TODO:
 			
 			String value; 
 			if (values[i] instanceof Clob) {
 				Clob clob = (Clob) values[i];
 				try {
 					value = clob.getSubString(1, (int) clob.length());
 				} catch (SQLException e) {
 					throw new RuntimeException(e);
 				}
 			} else {
 				value = String.valueOf(values[i]);
 			}
 			out.append("\"" + sanitise(value) + "\""); //TODO:
 			if (i < cols.length - 1) out.append(','); //TODO:
 		}
 		out.append('}'); //TODO:
 	}
 	
 	public void writeRowSeparator() throws IOException {
 		out.append(",\n"); //TODO:
 	}

 	public void writeUpdateCount(int updateCount) throws IOException {
 		out.append(String.valueOf(updateCount)); //TODO:
 	}
 	
 	public static String sanitise(String s) {
 		// escape backslash, double quotes and newlines
 		return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
 	}
 }

  /******************************************************************************
   * TODO:WQM : should be factored out when WQM is not dependant of Servelt code anymore.
   *Changed from type from Servletresponse to StringBuffer
   *****************************************************************************/
   class BlobResultWriter implements ResultWriter {
//  	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
  	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
  	
  	protected StringBuffer response;
  	
  	protected final String TYPE = "blob/clob"; // Just used for error messages
  	
  	protected static final int BUFFER_SIZE = 1024 * 4; // 4KB
  	
  	private static final String DATA_COL = "data";
  	private static final String CONTENT_TYPE_COL = "content_type";
  	private static final String FILENAME_COL = "filename";
  	
  	protected int dataIndex = -1;

  	protected int contentTypeIndex = -1;

  	protected int filenameIndex = -1;
  	
  	public BlobResultWriter(StringBuffer response) {
  		this.response = response;
  	}

  	public void writeHeader(String[] cols) throws Exception {
  		for (int i = 0; i < cols.length; i++) {
  			String colname=cols[i].toLowerCase();
  			if (colname.equals(DATA_COL))         dataIndex = i;
  			if (colname.equals(CONTENT_TYPE_COL)) contentTypeIndex = i;
  			if (colname.equals(FILENAME_COL))     filenameIndex = i;
  		}
  		// Data column must be present
  		if (dataIndex == -1) {
  			throw new Exception("DATA column must be present for " + TYPE + " query");
  		}
  	}

  	public void writeRow(Object[] values) throws IOException, Exception {
  		String contentType = getDefaultContentType();
  		if (contentTypeIndex != -1) {
  			contentType = (String) values[contentTypeIndex];
  		}
  		//TODO:WQM:response.setContentType(contentType);
  		response.append("Content-Type: "+contentType);
  		
  		if (filenameIndex != -1) {
  			//TODO:WQM: response.setHeader("Content-disposition", "inline; " + "filename=\"" + values[filenameIndex] + "\"");
  			response.append("Content-disposition: "+"inline; " + "filename=\"" + values[filenameIndex] + "\"");
  		}
  		
  		writeObject(values[dataIndex]);
  	}
  	
  	protected void writeObject(Object o) throws Exception {
  		Blob blob = (Blob) o;
  		// Commenting out the unused InputStream until this is rewritten,
  		// as noted just below.
  		//InputStream in;
  		try {
            //in = blob.getBinaryStream();
            blob.getBinaryStream();
  		} catch (SQLException e) {
  			throw new RuntimeException(e);
  		}
  	    //TODO: To be rewritten properly. need to investigate !!! 
  		throw new Exception("Writing BLOB on GDB WWWW Server  is not yet supported!");
  		//TODO: copyBinaryData(in, response.getOutputStream());  		
  	}

  	public void writeRowSeparator() throws IOException, Exception {
  		// There should only ever be one row written
  		//TODO:WQM: throw new ServletException("Cannot return multiple " + TYPE + "s from 1 query");
  		throw new Exception("Cannot return multiple " + TYPE + "s from 1 query");
  	}
  	
  	public void writeFooter() throws IOException {
  		// Do nothing
  	}

  	public void writeUpdateCount(int updateCount) throws IOException, Exception {
  	    //TODO:WQM: throw new ServletException("Broken query: should not use " + TYPE + " response format unless the query returns a ResultSet");
  		throw new  Exception("Broken query: should not use " + TYPE + " response format unless the query returns a ResultSet");
  	}
  	
  	protected static void copyBinaryData (InputStream in, OutputStream out) throws IOException {
  		byte[] buffer = new byte[BUFFER_SIZE];
  		int byteCount;
  		while ( (byteCount = in.read(buffer)) != -1 ) {
  			out.write(buffer, 0, byteCount);
  		}
  	}
  	
  	protected String getDefaultContentType() {
  		return "application/octet-stream";
  	}
  }
