/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTIEnvironment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import sun.misc.BASE64Encoder;

import com.ibm.gaiandb.GaianChildRSWrapper;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.RowsFilter;
import com.ibm.gaiandb.SecurityManager;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.policyframework.SQLResultFilter;
import com.ibm.gaiandb.policyframework.SQLResultFilterX;

/**
 * TODO
 * 
 * ICA REST VTI - uses a URL defined in a .icarest file, combined with
 * some properties in the gaindb_config.properties file to query an ICA REST
 * endpoint and return the results in a tabular format.
 * 
 * Sample config lines in gaiandb_config.properties:
 * =================================================
 *  
 * com.ibm.db2j.ICAREST.search.schema=DTITLE VARCHAR(100), DURL VARCHAR(1000), DNUM INT, RELEVANCE DOUBLE, DCONTEXT CLOB(32K)
 * com.ibm.db2j.ICAREST.search.url=http://localhost:8394/api/v10/search?output=application/xml&scope=All&results=1250
 * 
 * com.ibm.db2j.ICAREST.doctext.schema=DOCUMENT CLOB(10M)
 * com.ibm.db2j.ICAREST.doctext.url=http://localhost:8394/api/v10/search/preview?query=search&collection=
 * 
 * com.ibm.db2j.ICAREST.docbytes.schema=DOCUMENT BLOB(10M)
 * com.ibm.db2j.ICAREST.docbytes.url=http://localhost:8393/search/ESFetchServlet?cid=
 * 
 * Sample variable URI config:
 * ===========================
 * 
 * In file search.icarest: &query=$1
 * In file doctext.icarest: $1
 * in file docstream.icarest: $1
 * 
 * Sample queries:
 * ===============
 * 
 * Search for documents containing the word 'Arms'
 * select * from new com.ibm.db2j.ICAREST('search,Arms') IR
 * 
 * Extract the doctext for the DOC
 * select * from new com.ibm.db2j.ICAREST('doctext,HUMINT&uri=file:///C:/%2524user/VMData/EDAData/HUMINT/HUMINT%2BDoha%2BTalks%2Bon%2BDarfur.doc') IR
 *  
 * @author  Stephen Nicholas / Ed Jellard / David Vyvyan
 * 
 */
public class ICAREST extends AbstractDurableCacheVTI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";

	private static final Logger logger = new Logger( "ICAREST", 20 );

	private static final String FETCH_BUFFER_SIZE = "fetchbuffersize";
	private static final int FETCH_BUFFER_SIZE_DEFAULT = 20;
	
	private static final int FETCH_BATCH_SIZE_DEFAULT = 10;
	
	//Used to notify the bufferPopulator thread when runQuery has finished fetching results.
	private static final String END_OF_RESULTS_IDENTIFIER = "END_OF_RESULTS_IDENTIFIER";
	
	private static final String FUNCTION_COUNT = "count";
	private static final String FUNCTION_SEARCH = "search";
	private static final String FUNCTION_DOCTEXT = "doctext";
	private static final String FUNCTION_DOCBYTES = "docbytes";
	
	private static final String RESULTS_QUERY_PARAM = "&results=";
	private static final String START_QUERY_PARAM = "&start=";
	
	private static final Hashtable<String, String> schemas = new Hashtable<String, String>() {
		private static final long serialVersionUID = 1L;
	{
		put( FUNCTION_COUNT, "DCOUNT BIGINT, CACHEID INT" );
		put( FUNCTION_SEARCH, "DTITLE VARCHAR(256), DURL VARCHAR(1000), DNUM INT, RELEVANCE DOUBLE, DCONTEXT CLOB(32K), CACHEID INT" );
		put( FUNCTION_DOCTEXT, "DSIZE INT, DTEXT CLOB(10M), CACHEID INT" );
		put( FUNCTION_DOCBYTES, "DNAME VARCHAR(256), DTYPE VARCHAR(50), DSIZE INT, DBYTES BLOB(10M), CACHEID INT" );
	}};
	
	// Primary keys are mainly used here to avoid writing duplicates
	private static final Hashtable<String, String> primaryKeys = new Hashtable<String, String>() {
		private static final long serialVersionUID = 1L;
	{
		put( FUNCTION_COUNT, "CACHEID" );
		put( FUNCTION_SEARCH, "CACHEID, DNUM" );
		put( FUNCTION_DOCTEXT, "CACHEID" );
		put( FUNCTION_DOCBYTES, "CACHEID" );
	}};
	
	private static final String cacheExpirySeconds = "60";
	
	private static final String PROP_URL = "url";
	
	private static final String ICA_FETCHER_NAME = "ICARESTFetcher";
	private static final String BUFFER_POPULATOR_NAME = "BufferPopulator";
	
	
//	private static Map<String, AtomicLong> completedQueryCaches = new ConcurrentHashMap<String,AtomicLong>();
//	private static Map<String, Lock> cacheLockMap = new ConcurrentHashMap<String, Lock>();
	
	private int currentRow = 0;
		
	private NodeList results = null;
	private int totalResults = 0, startIndex = 0, itemsPerPage = 0;
	
	private String urlString = null;
	private String docName = null, docType = null, docText = null;
	private byte[] docBytes = null;
	
	private final String vtiArgs;
	
	private int fetchBatchSize = FETCH_BATCH_SIZE_DEFAULT;
	private int fetchBufferSize = FETCH_BUFFER_SIZE_DEFAULT;
	private final BlockingDeque<DataValueDescriptor[][]> fetchBuffer;
	private final DataValueDescriptor[] resultRowTemplate;
	
	private DataValueDescriptor[][] currentResultBatch;
	private int currentResultBatchIndex = 0;
	
	private boolean policyFilterDefined = false;
	private SQLResultFilter sqlResultFilter;
	private SQLResultFilterX sqlResultFilterX;
	
	private final BlockingDeque<String> bufferPopulatorWorkQueue;
	
	private int maxSourceRows = -1;
	
	private int newStartIndex;
	
	private boolean queryRunning = false;
	
	private Qualifier[][] qualifiers;
	
	private Map<String, Integer> cacheErrors = new Hashtable<String, Integer>();
	private Map<String, Object []> fieldModificationCountMap = new Hashtable<String, Object []>();
	
	public Hashtable<String, String> getDefaultVTIProperties() {
		
		if ( null == defaultVTIProperties ) {
		
			Hashtable<String, String> props = super.getDefaultVTIProperties();
			String prefix = getPrefix();
			
			if(schemas.containsKey(prefix)) {
				props.put(PROP_SCHEMA, schemas.get(prefix));
			}
			
			// Do not define default properties for the URLs because we want the ICAREST to be disabled when URLs are not defined in the config file.
			// props.put(PROP_URL, urls.get(prefix));
			props.put(PROP_CACHE_EXPIRES, cacheExpirySeconds);
			
			if(primaryKeys.containsKey(prefix)) {
				props.put(PROP_CACHE_PKEY, primaryKeys.get(prefix));
			}
						
			defaultVTIProperties = props;
		}
		
		return defaultVTIProperties;
	}
	
	private int instanceId;
	
	private boolean isSearch = false;
	private boolean isCount = false;
	private boolean isDocBytes = false;
	private boolean isDocText = false;

	private boolean cachingExplicitlyDisabled = false;
	
    public ICAREST(String vtiArgs) throws Exception {
		super(vtiArgs);
		
		this.vtiArgs = vtiArgs;
		this.instanceId = this.hashCode();
				
		/*
		 * What kind of thing are we doing? 
		 */
		if(FUNCTION_SEARCH.equals(getPrefix())) {
			isSearch = true;
		}
		else if(FUNCTION_COUNT.equals(getPrefix())) {
			isCount = true;
		}
		else if(FUNCTION_DOCBYTES.equals(getPrefix())) {
			isDocBytes = true;
		}
		else if(FUNCTION_DOCTEXT.equals(getPrefix())) {
			isDocText = true;
		}
		else {
			logger.logWarning(GDBMessages.DSWRAPPER_ICAREST_INVALID_FUNCTION_ERROR, "The function type: '" + getPrefix() 
					+ "' is not recognised by ICAREST.");
			throw new Exception("The function type: '" + getPrefix() + "' is not recognised by ICAREST.");
		}
		
        logger.logImportant("Entered ICAREST(vtiArgs), function: '" + getPrefix() 
				+ "', args: '" + replacements + "', instance id: '" + instanceId + "'.");
		
		/*
		 * Check whether the URL for the function type is specified.
		 */
		String url = "";
		
		try {
			url = getVTIPropertyWithReplacements(PROP_URL);
		}
		catch(Exception e) {
			logger.logException(GDBMessages.DSWRAPPER_ICAREST_URL_PARAMETER_NOT_SPECIFIED, 
					"An error occurred while attempting to read in the URL to query for this function type. "
					+ "This should be specified by the ICAREST." + getPrefix() + "." + PROP_URL + " parameter in the config file. "
					+ "There is no default value that can be used.", e);
		}
		
		
		/*
		 * Is caching explicitly disabled?
		 */
		cachingExplicitlyDisabled = 0 >= getExpiryDuration();

		
		/*
		 * Get the batch size to fetch from the db and filter rows in.
		 * Use the value of &results= from the query param, if set - else will use default.
		 */
		try {
			String resultsRegex = "\\Q" + RESULTS_QUERY_PARAM + "\\E(\\d+)";
			Matcher resultsMatcher = Pattern.compile(resultsRegex).matcher(url);
			if(resultsMatcher.find()) {
				
				int resultsSpecifiedInUrl = Integer.parseInt(resultsMatcher.group(1));
				
				if(resultsSpecifiedInUrl > 0) {
					fetchBatchSize = resultsSpecifiedInUrl;
				}
			}
		} catch(Exception e) {
			//Just log - default will be used
			logger.logException(GDBMessages.DSWRAPPER_ICAREST_INVALID_FETCH_BATCH_SIZE_PARAMETER, 
					"An error occurred while attempting to read in the value for the fetch batch size. " +
					"This should be specified by the &result query parameter on the url. " +
					"The default value (" + FETCH_BATCH_SIZE_DEFAULT + ") will be used.", e);
		}
		
		
		/*
		 * Build and set a custom value for 'extension'.
		 * The extension is 'ICAREST_' + MD5(vtiArgs + result schema + fetchBatchSize). This ensures it is unique for this particular query and
		 * will be the same when it is repeated. It's not just for this instance of ICAREST.
		 * This ensures each different query is segregated into it's own cache, meaning that the results can be shared between
		 * invocations, but avoiding the scenario of an ever growing cache.
		 */
		try {
			byte [] digest = SecurityManager.getChecksumMD5((vtiArgs + getVTIProperty(PROP_SCHEMA) + fetchBatchSize).getBytes());
			StringBuilder sb = new StringBuilder();
			for (byte b : digest) {
				sb.append(Integer.toHexString(0x100 + (b & 0xff)).substring(1));
			}
		
			this.setExtension(this.getClass().getSimpleName() + "_" + sb.toString());
		}
		catch(NoSuchAlgorithmException e) {
			//MD5 not available
		}
		
		
		/*
		 * Get the size of the fetch buffer (how many fetch batches to have in memory; subsequent ICAREST requests
		 * will wait until the buffer has more space).
		 * Use the value of ICAREST.fetchbuffersize from the config, if set - else will use default. 
		 */
		try {
			
			String vtiProperty = getVTIProperty(FETCH_BUFFER_SIZE);
			
			int fetchBufferSizeFromConfig = Integer.parseInt(vtiProperty);
			
			if(fetchBufferSizeFromConfig > 0) {
				fetchBufferSize = fetchBufferSizeFromConfig;
			}
		}
		catch (NumberFormatException e) {
			//Just log exception - default will be used
			logger.logException(GDBMessages.DSWRAPPER_ICAREST_INVALID_FETCH_BUFFER_SIZE_PARAMETER, 
					"An error occurred while attempting to read in the value for the fetch buffer size. " +
					"This should be specified by the ICAREST." + FETCH_BUFFER_SIZE + " parameter in the config file. " +
					"The default value (" + FETCH_BUFFER_SIZE_DEFAULT + ") will be used.", e);
		}
		catch(Exception e) {
			//Just log (at low level) - default will be used
			logger.logDetail("No value has been specified for the ICAREST." + FETCH_BUFFER_SIZE + " parameter in the config file. " +
					"The default value (" + FETCH_BUFFER_SIZE_DEFAULT + ") will be used.");
		}
		
		// Create bucket to fill with results for derby to fetch from (a fetch buffer)
		fetchBuffer = new LinkedBlockingDeque<DataValueDescriptor[][]>( fetchBufferSize);
		
		//Instantiate the work queue for the buffer populator
		bufferPopulatorWorkQueue = new LinkedBlockingDeque<String>();
		
		
		//Get the DVDR[] template for result rows
		resultRowTemplate = getMetaData().getRowTemplate();
		
		
		// encode incoming uris (from vtiArgs) for doctext and docbytes functions
		if ( isDocBytes || isDocText ) {
			
			// constructor args contains url arg labels - these need splitting up
			if ( 1 > replacements.size() )
				throw new Exception("Unable to execute ICAREST function '" + getPrefix() + "': Missing URI argument in VTI constructor");
			
			// Don't encode the bits before the actual URI. i.e. "&collection=<collection>&uri=" must be unchanged
			String arg = replacements.get(0);
			String uriTag = "&uri=";
			int uriTagIndex = arg.indexOf( uriTag );
			String collection = -1 == uriTagIndex ? "" : arg.substring( 0, uriTagIndex );
			String uri = -1 == uriTagIndex ? arg : arg.substring( uriTagIndex + uriTag.length() );
			
			replacements.set(0, collection + uriTag + URLEncoder.encode( uri, Charset.defaultCharset().name() ));
		} else {			
			// No url arg labels (e.g. '&uri') in the replacements - encode args fully
			for( int i=0; i<replacements.size(); i++ )
				replacements.set(i, URLEncoder.encode( replacements.get(i), Charset.defaultCharset().name() ));
		}
		
		//Load the SQL result filter - if there is one
		sqlResultFilter = GaianDBConfig.getSQLResultFilter();
		
		//If there's a filter
		if(sqlResultFilter != null) {
			policyFilterDefined = true;
			
			//If it's a ...FilterX - assign vars appropriately
			if(sqlResultFilter instanceof SQLResultFilterX) {
				
				sqlResultFilterX = (SQLResultFilterX)sqlResultFilter;
				sqlResultFilter = null;
				
				//Also, ask the policy for the max source rows to return
				maxSourceRows = sqlResultFilterX.setDataSourceWrapper(vtiClassName);
			}
		}
	}
	
	public boolean executeAsFastPath() throws StandardException, SQLException {
		logger.logInfo("Entered executeAsFastPath()");
		
		if(queryRunning) {
			logger.logImportant("The query is already running - no need to re-execute.");
		}
		else {			
			//Kick off the query worker thread
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					logger.logInfo("Query worker thread started");
					runQuery();
					logger.logInfo("Query worker thread ended");
				}
			}, ICA_FETCHER_NAME + " for ICAREST instance " + instanceId).start();
			
			/*
			 * If caching has not been disabled:
			 * 		Kick off the buffer populator thread.
			 * Else:
			 * 		We don't need the buffer populator, as there's nowhere to cache ICAREST results.
			 * 		FetchBuffer population will halt when it's full.
			 */
			if(!cachingExplicitlyDisabled) {
				new Thread(new Runnable() {
					
					@Override
					public void run() {
						logger.logInfo("Buffer populator thread started");
						populateBuffer();
						logger.logInfo("Buffer populator thread ended");
					}
				}, BUFFER_POPULATOR_NAME + " for ICAREST instance " + instanceId).start();
			}
		}
		
		return true; // never return false - derby calls executeQuery() if you do

	}
	
	public int nextRow(DataValueDescriptor[] dvdr) throws StandardException, SQLException {
		
		/*
		 * Loop until we get a valid row (that matches any qualifiers).
		 * Or until end of results (when we return directly).
		 */
		boolean gotRow = false;
		
		while(!gotRow) {
		
			/*
			 * While we don't have a batch from the buffer
			 */
			while(currentResultBatch == null || currentResultBatchIndex >= currentResultBatch.length) {
				
				try {
					currentResultBatch = fetchBuffer.takeFirst();
					currentResultBatchIndex = 0;
									
					/*
					 * If:
					 * 	- the query is flagged as no longer running 
					 *  - AND we get an empty batch 
					 *  - AND the fetch buffer is now empty
					 * Then we've reached the end of results 
					 */
					//
					if ( 0 == currentResultBatch.length )
						logger.logInfo("====>>>>  takeFirst() returned empty batch. queryRunning: " + queryRunning + ", fetchBuffer.size(): " + fetchBuffer.size());
					
					if(!queryRunning && currentResultBatch.length == 0 && fetchBuffer.isEmpty()) {
						return IFastPath.SCAN_COMPLETED;
					}
				} catch (InterruptedException e) {
					logger.logException( GDBMessages.ENGINE_NEXT_ROW_ERROR, "Caught Exception in nextRow() (returning SCAN_COMPLETED): ", e );
					return IFastPath.SCAN_COMPLETED;
				}
			}
			
			/*
			 * At this point we should have the next non-empty batch.
			 * If qualifiers are set, test whether this row matches:
			 * 		If so: Copy into dvdr & flag to return
			 * 		Else: Loop around again looking for another row that is good.
			 */
			DataValueDescriptor[] currentResult = currentResultBatch[currentResultBatchIndex];
			
			if(qualifiers == null || RowsFilter.testQualifiers( currentResult, qualifiers )) {
				System.arraycopy(currentResult, 0, dvdr, 0, currentResult.length);
				gotRow = true;
			}
			
			currentResultBatchIndex++;
		}
	

		return IFastPath.GOT_ROW;
	}
	
	public int getRowCount() throws Exception {
//		return doc.getElementsByTagName("es:result").getLength();
		return results.getLength();
	}
	
//	public InputStream getDocStreamResult() {
//		return docStream;
//	}

	public void setQualifiers(VTIEnvironment vtie, Qualifier[][] qual) throws SQLException {
		//Set the qualifiers for use in nextRow later
		this.qualifiers = qual;
	}
	
	public boolean isBeforeFirst() {
		return 0 == currentRow;
	}	
	
//SDN - Not sure if required - 04/12
//	public class MyResetableStream extends InputStream implements Resetable {
//
//		private InputStream is = null;
//		
//		private int[] cachedBytes = new int[1000];
//		private int numBytesCached = 0, pos = 0;
//		
//		public MyResetableStream(InputStream is) {
//			super();
//			this.is = is;
//			logger.logInfo("Entered MyResetableStream(); Mark isSupported ? " + is.markSupported());
////			if ( is.markSupported() ) is.mark(1000);
//		}
//
//		public void closeStream() {
//			logger.logInfo("Entered closeStream()");
//			try {
//				is.close();
//			} catch (IOException e) {
//				logger.logException(GDBMessages.DSWRAPPER_STREAM_CLOSE_ERROR_IO, "Unable to close Stream", e);
//			}
//		}
//
//		public void initStream() throws StandardException {
//			logger.logInfo("Entered initStream()");
//			numBytesCached = 0; pos = 0;
//		}
//
//		public void resetStream() throws IOException, StandardException {
//			logger.logInfo("Entered resetStream(), numBytesCached = " + numBytesCached);
////			if ( is.markSupported() ) is.reset();
//			pos = 0;
//		}
//
//		@Override
//		public int read() throws IOException {
//			logger.logInfo("Entered read()");
//			if ( pos < numBytesCached ) return cachedBytes[pos++];
//			int b = is.read();
//			if ( numBytesCached < cachedBytes.length ) cachedBytes[numBytesCached++] = b;
//			return b;
//		}		
//	}


	public double getEstimatedCostPerInstantiation(VTIEnvironment arg0)
			throws SQLException {
		return 0;
	}

	public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
		return 1; // encourage Derby to treat this VTI as the inner table in joins
	}

	public boolean supportsMultipleInstantiations(VTIEnvironment arg0)
			throws SQLException {
		return false;
	}
	
	@Override
	public void close() throws SQLException {
		super.close();
		markCacheInUse(false);
		cleanUpCaches();
	}

	
	
	
	private void runQuery() {
		queryRunning = true;
		
		try {
			// Get the given VTI property, and substitute any arguments $0, $1, $2,.. with values passed in to the VTI call.
			//urlString = escapePercentSymbolsAndSpaces( getVTIPropertyWithReplacements( PROP_URL ) );
			urlString = getVTIPropertyWithReplacements(PROP_URL); // For doctext and docbytes, URI is now encoded in the constructor
			
		} catch (Exception e) {
			logger.logImportant("Ignoring ICAREST '"+getPrefix()+"' query: " + e.getMessage());
			
			//Notify the buffer populator that we've finished querying ICAREST 
			//by putting the END_OF_RESULTS_IDENTIFIER to the buffer populator work queue
			synchronized (bufferPopulatorWorkQueue) {
				
				/*
				 * Loop trying to put to the queue - this is to avoid us stopping due to InterruptedException.
				 * WARNING: This 'could' potentially loop forever.
				 */
				boolean offerSuccess = bufferPopulatorWorkQueue.offerLast(END_OF_RESULTS_IDENTIFIER);
				
				while(!offerSuccess) {
					try {
						offerSuccess = bufferPopulatorWorkQueue.offerLast(END_OF_RESULTS_IDENTIFIER, 100, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e1) {
						//Don't care
					}
				}
			}
			queryRunning = false;
			
			//Return - as no point trying to do any work
			return;
		}
		
		//Mark cache as logically in use
		markCacheInUse(true);
		
		cacheErrors.clear();
		fieldModificationCountMap.clear();
		
		Lock cacheModifyLock = null;
		
		if(!cachingExplicitlyDisabled) {
		
			cacheModifyLock = getCacheModifyLock();
		
			/* 
			 * Attempt to get the cache modify lock for this query.
			 * 
			 * Note: it doesn't matter who gets here first. The first 
			 * guy will do the heavy lifting and subsequent folks will 
			 * wait on the lock and then read from the cache when free.
			 */
			cacheModifyLock.lock();
		}
		
		try{
		
			newStartIndex = 0;
			currentRow = 0;
			
			//Flag of whether any data was cached as part of this query
			boolean dataCached = false;
			
			while(queryRunning) {
	
				/*
				 * Build the urlToUse for this part of the query
				 * NOTE: we do this here so that the cache check works correctly.
				 */
				String urlToUse = urlString;
				
				//If doing COUNT
				//Only need to request 0 results - as can use totalResults value from return
				if (isCount) {
					int indexOfResultsParam = urlToUse.indexOf(RESULTS_QUERY_PARAM);
					
					if(indexOfResultsParam == -1) {
						urlToUse = urlToUse + RESULTS_QUERY_PARAM + 0;
					}
					else {
						urlToUse = urlToUse.replaceAll("\\Q" + RESULTS_QUERY_PARAM + "\\E\\d*", RESULTS_QUERY_PARAM + 0);
					}
				}
				//If doing SEARCH
				//Need to add batch size and start index
				else if(isSearch) {
					/*
					 * Add rowBatchSize to query as &results= (overwriting any existing value).
					 */				
					int indexOfResultsParam = urlToUse.indexOf(RESULTS_QUERY_PARAM);
					
					if(indexOfResultsParam == -1) {
						urlToUse = urlToUse + RESULTS_QUERY_PARAM + fetchBatchSize;
					}
					else {
						urlToUse = urlToUse.replaceAll("\\Q" + RESULTS_QUERY_PARAM + "\\E\\d*", RESULTS_QUERY_PARAM + fetchBatchSize);
					}
					
					/*
					 * Add startIndex to query as &start= (overwriting any existing value).
					 */
					
					int indexOfStartParam = urlToUse.indexOf(START_QUERY_PARAM);
					
					if(indexOfStartParam == -1) {
						urlToUse = urlToUse + START_QUERY_PARAM + newStartIndex;
					}
					else {
						urlToUse = urlToUse.replaceAll("\\Q" + START_QUERY_PARAM + "\\E\\d*", START_QUERY_PARAM + newStartIndex);
					}
				}
				
				/*
				 * Create a reusable array to fill with batches of results to work with.
				 */
				DataValueDescriptor[][] resultBatch = new DataValueDescriptor[fetchBatchSize][];
			
				for (int i=0; i < resultBatch.length; i++) {
					//Create a new 'row'
					DataValueDescriptor[] nextRow = new DataValueDescriptor[resultRowTemplate.length];
					
					//Fill the new row with empty copies of every DataValueDescriptor type in the rowTemplate
					for ( int j=0; j < resultRowTemplate.length; j++ ) {
						nextRow[j] = resultRowTemplate[j].getNewNull();
					}
					
					//Place the new holder row into the result batch
					resultBatch[i] = nextRow;
				}
				
				/*
				 * Get the results.
				 */
				int resultsInThisBatchBeforeFiltering = 0;
				
				try {
					if ( isCached( "CACHEID="+urlToUse.hashCode() ) ) {
						logger.logImportant("Data is cached - no need to run ICAREST query");
	
						// While more results from cache && we've not hit the batch limit
						while( resultsInThisBatchBeforeFiltering < fetchBatchSize && (nextRowFromCache(resultBatch[resultsInThisBatchBeforeFiltering])) != SCAN_COMPLETED) {
							resultsInThisBatchBeforeFiltering++;
							currentRow++;
						}
						
						//If search, then need to inflate start index - as if we were doing a real query
						if(isSearch) {
							newStartIndex = newStartIndex + resultsInThisBatchBeforeFiltering;
						}
					}
					else {
						if(isSearch) {
							
							//If max source rows hit - don't bother to query
							//Note: there is also a check further down - as we may hit the limit in the middle of a batch of results
							if(-1 < maxSourceRows && currentRow >= maxSourceRows) {
								logger.logWarning(GDBMessages.DSWRAPPER_ICAREST_PARTIAL_RESULT, "The raw ICAREST Query has been restricted to a maximum of " +
										maxSourceRows + " results. (search top)");
							}
							else {
								logger.logImportant("Opening URL: " + urlToUse);
								URLConnection urlc = new URL(urlToUse).openConnection();
								urlc.setRequestProperty("Accept-encoding", "gzip,deflate");
								
								String usr = getVTIPropertyNullable("username");
								String pwd = getVTIPropertyNullable("password");
								
								if ( null != usr && null != pwd ) {
//									Use this code for Authentication? (NOT TESTED YET)
									String encoded = new String( new BASE64Encoder().encode(new String( usr + ':' + pwd ).getBytes()) );
									urlc.setRequestProperty("Proxy-Authorization", "Basic " + encoded);
								} else
									logger.logInfo("Unable to find username and password properties for ICAREST. No authentication parms will be passed");
																
								Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(urlc.getInputStream());
								
								itemsPerPage = Integer.parseInt( doc.getElementsByTagName("es:itemsPerPage").item(0).getTextContent() );
								totalResults = Integer.parseInt( doc.getElementsByTagName("es:totalResults").item(0).getTextContent() );
								
								if ( 1 > itemsPerPage ) {
									logger.logImportant("Retrieved " + itemsPerPage + " search results, totalResults = " + totalResults);
								}
		
								//Note: startIndex may not be present - if we've hit the end of the results
								NodeList startIndexElements = doc.getElementsByTagName("es:startIndex");
								
								if(startIndexElements.getLength() > 0) {
									startIndex = Integer.parseInt( doc.getElementsByTagName("es:startIndex").item(0).getTextContent() ); // inclusive
		
									newStartIndex = startIndex + itemsPerPage;	
								}
								//Else we've hit the end of the results - no need to increment startIndex
								
								results = doc.getElementsByTagName("es:result");
								
								logger.logImportant("Retrieved " + itemsPerPage + " search results, records " +
										(startIndex+1) + "-" + (startIndex+itemsPerPage) + " of " + totalResults + " results.");
													
							
								// While more results 
								// AND we've not hit the batch limit
								while(resultsInThisBatchBeforeFiltering < fetchBatchSize && resultsInThisBatchBeforeFiltering < results.getLength()) {
	
									// If we've hit the max source rows limit - warn and stop looping
									// Note: there's also a check above to rule out queries if we can
									if(-1 < maxSourceRows && currentRow >= maxSourceRows) {
										logger.logWarning(GDBMessages.DSWRAPPER_ICAREST_PARTIAL_RESULT, "The raw ICAREST Query has been restricted to a maximum of " + 
												maxSourceRows + " results. (search mid)");
										break;
									}
									else {
									
										Node item = results.item(resultsInThisBatchBeforeFiltering);
										
										NodeList iel = item.getChildNodes();
										
										//Set CACHEID
										resultBatch[resultsInThisBatchBeforeFiltering][5].setValue( urlToUse.hashCode() );
										
										int numFieldsFound = 0;
										boolean isFoundCollectionName = false;
										String icaURI = null, collectionName = null;
										
										for (int k = 0; k < iel.getLength() && 5 > numFieldsFound; k++) {
											
											if (iel.item(k).getNodeType() == Element.ELEMENT_NODE) {
												Element child = (Element) iel.item(k);
												String tagName = child.getTagName();
																					
												if ( tagName.equals("es:title") ) 			resultBatch[resultsInThisBatchBeforeFiltering][0].setValue(child.getTextContent());
												else if ( tagName.equals("es:relevance") ) 	resultBatch[resultsInThisBatchBeforeFiltering][3].setValue(child.getTextContent());
												else if ( tagName.equals("es:summary") )	resultBatch[resultsInThisBatchBeforeFiltering][4].setValue(child.getTextContent());
												else if ( tagName.equals("es:id") )			icaURI = child.getTextContent();
												else if ( isFoundCollectionName ) continue;
												else {
													// Search for collection name
													
													String id =
														tagName.equals("es:link") && child.getAttribute("rel").equals("alternate") ? child.getAttribute("href") :
														tagName.equals("es:thumbnail") ? child.getAttribute("href") :
														tagName.equals("es:link") && child.getAttribute("rel").equals("via") ? child.getAttribute("href") :
														null;
														
													if ( null == id ) continue;
														
													int idx = id.indexOf('?');
													if ( -1 == idx ) {
														logger.logWarning(GDBMessages.DSWRAPPER_DOC_URI_NOT_FOUND, "Unable to find doc URI in an href link of search results, tag: " + tagName + ", href: " + id);
														continue;
													}
													
													String urlArgs = id.substring(idx+1);
													String uriTag = "&uri=";
													int idxURI = urlArgs.indexOf(uriTag);
													if ( -1 == idxURI ) {
														logger.logWarning(GDBMessages.DSWRAPPER_URI_ARG_NOT_FOUND, "Unable to find 'uri' argument in href link, tag: " + tagName + ", href: " + id);
														continue;
													}
													
													String collectionTag = "collection=";
													
													collectionName = urlArgs.startsWith(collectionTag) ? urlArgs.substring(collectionTag.length(), idxURI) : null;
													
													if ( null == collectionName ) {
														// 'collection' is not the first argument
														collectionTag = '?' + collectionTag;
														int idxCol = urlArgs.indexOf(collectionTag);
														if ( -1 == idxCol ) {
															logger.logWarning(GDBMessages.DSWRAPPER_COLLECTION_ARG_NOT_FOUND, "Unable to find 'collection' argument in href link, tag: " + tagName + ", href: " + id);
															continue;
														}
														collectionName = urlArgs.substring( idxCol + collectionTag.length(), idxURI );
													}							
													isFoundCollectionName = true;
												}
												numFieldsFound++; // increment numFields found only if we found a new field (otherwise 'continue;' will have been called)
											} // if the child is an element node
										} // for all children
			
										resultBatch[resultsInThisBatchBeforeFiltering][1].setValue( collectionName + "&uri=" + icaURI); // return this as URI for now - later the collection and uri should be separated
										resultBatch[resultsInThisBatchBeforeFiltering][2].setValue( icaURI.hashCode() );
										
										//Validate and modify the result row so it matches the resultset schema
										validateAndModifyToSchema(resultBatch[resultsInThisBatchBeforeFiltering], fieldModificationCountMap);
										
										cacheRow(resultBatch[resultsInThisBatchBeforeFiltering], cacheErrors);
										dataCached = true;
										
										resultsInThisBatchBeforeFiltering++;
										
										currentRow++;
									}
								}			
							}
						}
						else if (isCount) {
							
							//Note: urlString will have been modified before cache check
							logger.logImportant("Opening URL: " + urlToUse);
							URLConnection urlc = new URL(urlToUse).openConnection();
							urlc.setRequestProperty("Accept-encoding", "gzip,deflate");
							
							Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(urlc.getInputStream());
							
							int count = Integer.parseInt( doc.getElementsByTagName("es:totalResults").item(0).getTextContent() );
							
							resultBatch[resultsInThisBatchBeforeFiltering][0].setValue(count);
							resultBatch[resultsInThisBatchBeforeFiltering][1].setValue(urlToUse.hashCode());
							
							//Validate and modify the result row so it matches the resultset schema
							validateAndModifyToSchema(resultBatch[resultsInThisBatchBeforeFiltering], fieldModificationCountMap);
							
							cacheRow(resultBatch[resultsInThisBatchBeforeFiltering], cacheErrors);
							dataCached = true;
							
							resultsInThisBatchBeforeFiltering++;
							
							currentRow++;
							
						} else if (isDocText) {
							
							if(maxSourceRows == 0) {
								logger.logWarning(GDBMessages.DSWRAPPER_ICAREST_PARTIAL_RESULT, "The raw ICAREST Query has been restricted to a maximum of " +
										maxSourceRows + " results. (doctext)");
							}
							else {
								logger.logImportant("Opening URL: " + urlToUse);
								URLConnection urlc = new URL(urlToUse).openConnection();
								urlc.setRequestProperty("Accept-encoding", "gzip,deflate");
								
								final char[] buf = new char[0x10000];
								StringBuilder out = new StringBuilder();
								Reader in = new InputStreamReader(urlc.getInputStream());
								int numBytes;
								while ( (numBytes = in.read(buf, 0, buf.length)) > 0 ) {
									out.append(buf, 0, numBytes);
								}
								
								docText = out.toString();
								logger.logImportant("Retrieved doctext, num chars: " + docText.length());
								
								resultBatch[resultsInThisBatchBeforeFiltering][0].setValue( docText.length() );
								resultBatch[resultsInThisBatchBeforeFiltering][1].setValue( docText );
								resultBatch[resultsInThisBatchBeforeFiltering][2].setValue( urlToUse.hashCode() );
								
								//Validate and modify the result row so it matches the resultset schema
								validateAndModifyToSchema(resultBatch[resultsInThisBatchBeforeFiltering], fieldModificationCountMap);
								
								cacheRow(resultBatch[resultsInThisBatchBeforeFiltering], cacheErrors);
								dataCached = true;
								
								resultsInThisBatchBeforeFiltering++;
								
								currentRow++;
							}
							
						} else if (isDocBytes) {
							
							if(maxSourceRows == 0) {
								logger.logWarning(GDBMessages.DSWRAPPER_ICAREST_PARTIAL_RESULT, "The raw ICAREST Query has been restricted to a maximum of " +
										maxSourceRows + " results. (docbytes)");
							}
							else {
								logger.logImportant("Opening URL: " + urlToUse);
								URLConnection urlc = new URL(urlToUse).openConnection();
								urlc.setRequestProperty("Accept-encoding", "gzip,deflate");
								
								docName = null;
								
								String fInfo = urlc.getHeaderField("Content-Disposition");
								if ( null != fInfo ) { // we have a file name
									String fTag = "filename=";
									int idx = fInfo.indexOf(fTag);
									if ( -1 != idx ) {
										docName = fInfo.substring(idx+fTag.length()+1, fInfo.length()-1);
										logger.logInfo("Content-Disposition filename: " + docName);
									}
								}
								docType = urlc.getContentType();
								logger.logInfo("Content-type: " + docType);
								logger.logInfo("Content-encoding: " + urlc.getContentEncoding());
								
								ByteArrayOutputStream baos = new ByteArrayOutputStream();
								InputStream is = urlc.getInputStream();
								Util.copyBinaryData(is, new GZIPOutputStream(baos));
								docBytes = baos.toByteArray();
								baos.close(); // other streams are closed
								
								logger.logImportant("Retrieved and zipped doc bytes from ICA InputStream, numbytes = " + docBytes.length);
								
								logger.logInfo("Setting document docbytes row, numbytes " + docBytes.length);
								resultBatch[resultsInThisBatchBeforeFiltering][0].setValue( docName );
								resultBatch[resultsInThisBatchBeforeFiltering][1].setValue( docType );
								resultBatch[resultsInThisBatchBeforeFiltering][2].setValue( docBytes.length );
								resultBatch[resultsInThisBatchBeforeFiltering][3].setValue( docBytes );
								resultBatch[resultsInThisBatchBeforeFiltering][4].setValue( urlToUse.hashCode() );
								
								//Validate and modify the result row so it matches the resultset schema
								validateAndModifyToSchema(resultBatch[resultsInThisBatchBeforeFiltering], fieldModificationCountMap);
								
								cacheRow(resultBatch[resultsInThisBatchBeforeFiltering], cacheErrors);
								dataCached = true;
								
								resultsInThisBatchBeforeFiltering++;
								
								currentRow++;
							}
						}
					}
				} catch (Exception e) {
					logger.logException(GDBMessages.DSWRAPPER_ICAREST_ROW_FETCH_ERROR, "Unable to fetch row: ", e);
				}
				
				//Don't bother filtering if no results
				if(resultsInThisBatchBeforeFiltering != 0) {
					
					//If no caching
					//	put to buffer synchronously
					//	AND do filtering now
					if(cachingExplicitlyDisabled) {
						//If not a full batch - reduce the batch size to pass to the filter
						//Note: this should only happen at the tail end of the query - so no need to worry about re-expanding
						if(resultsInThisBatchBeforeFiltering < fetchBatchSize) {
		
							//Create temp reduced batch
							DataValueDescriptor[][] reducedBatch = new DataValueDescriptor[resultsInThisBatchBeforeFiltering][];
		
							//Copy just the filled rows into the reduced batch
							System.arraycopy(resultBatch, 0, reducedBatch, 0, resultsInThisBatchBeforeFiltering);
		
							//Re-assign the resultBatch to the reduced version
							resultBatch = reducedBatch;
		
							logger.logDetail("Batched Filtering: Reduced final filtering batch to size: " + resultBatch.length);
						}
		
						/*
						 * Filter the batch.
						 */
						DataValueDescriptor[][] rb = filterBatch(resultBatch);
						if ( null != rb ) resultBatch = rb;
						
					
						boolean offerSuccess = fetchBuffer.offerLast(resultBatch);
						
						while(!offerSuccess) {
							try {
								offerSuccess = fetchBuffer.offerLast(resultBatch, 100, TimeUnit.MILLISECONDS);
							} catch (InterruptedException e) {
								//Don't care
							}
						}
					}
					else {
						/*
						 * Notify the bufferPopulator it has work to do.
						 * Note: the buffer populator will do the filtering. 
						 */
						synchronized (bufferPopulatorWorkQueue) {	
							String toOffer = "" + urlToUse.hashCode();
							
							boolean offerSuccess = bufferPopulatorWorkQueue.offerLast(toOffer);
							
							while(!offerSuccess) {
								try {
									offerSuccess = bufferPopulatorWorkQueue.offerLast(toOffer, 100, TimeUnit.MILLISECONDS);
								} catch (InterruptedException e) {
									//Don't care
								}
							}
						}
					}
				}
				
				//If not a SEARCH (as all other queries should be complete by now)
				//OR (if search) If the batch was not filled (before filtering)
				//then we've hit the end of the results - query finished
				if(!isSearch || resultsInThisBatchBeforeFiltering < fetchBatchSize) {
					queryRunning = false;
				}
			}
			
			/*
			 * Warn if any of the data was modified to match the schema
			 */
			if(!fieldModificationCountMap.isEmpty()) {
				
				Set<Entry<String,Object[]>> entrySet = fieldModificationCountMap.entrySet();
				
				for (Entry<String, Object[]> entry : entrySet) {
					logger.logWarning(GDBMessages.DSWRAPPER_ICAREST_DATA_TRUNCATION_OCCURRED, "Truncation of field '" + entry.getKey() + "' to '" + entry.getValue()[0] 
							+ "' was performed; for ICAREST " + getPrefix() + " query. Occurences: " + entry.getValue()[1]);
				}
			}
			
			//If no caching - put end of results indicator batch to fetchbuffer
			if(cachingExplicitlyDisabled){
				putEndOfResultsIndicatorBatch();
			}
			else {
				/*
				 * If any result caching errors occurred - log warnings and invalidate the cache.
				 */
				if(!cacheErrors.isEmpty()) {
					
					//Log warning
					StringBuffer cacheErrorMessage = new StringBuffer();
					cacheErrorMessage.append("The following SQL errors occured while trying to cache the results (SQLState - Count): ");
					
					Set<Entry<String,Integer>> entrySet = cacheErrors.entrySet();
					
					for (Entry<String, Integer> entry : entrySet) {
						cacheErrorMessage.append(entry.getKey());
						cacheErrorMessage.append(" - ");
						cacheErrorMessage.append(entry.getValue());
						cacheErrorMessage.append(',');
						//Remove trailing comma
						cacheErrorMessage.setLength(cacheErrorMessage.length() - 1);
					}
					
					cacheErrorMessage.append("; for ICAREST ");
					cacheErrorMessage.append(getPrefix());
					cacheErrorMessage.append(" query.");
					
					logger.logWarning(GDBMessages.DSWRAPPER_ICAREST_CACHE_ROWS_ERROR, cacheErrorMessage.toString());
					
					//Also, invalidate the cache
					invalidateCache();
				}
				/*
				 * Else caching completed successfully
				 * If data was cached - and we didn't just read from the cache - update cache expiry values.
				 */
				else if(dataCached) {
					try {
						resetCacheExpiryTime();
					} catch (SQLException e) {
						logger.logException(GDBMessages.DSWRAPPER_ICAREST_EXECUTE_ERROR, "Unable to reset cache expiration time ", e);
					}
				}
				
				//Notify the buffer populator that we've finished querying ICAREST 
				//by putting the END_OF_RESULTS_IDENTIFIER to the buffer populator work queue
				synchronized (bufferPopulatorWorkQueue) {
					
					/*
					 * Loop trying to put to the queue - this is to avoid us stopping due to InterruptedException.
					 * WARNING: This 'could' potentially loop forever.
					 */
					boolean offerSuccess = bufferPopulatorWorkQueue.offerLast(END_OF_RESULTS_IDENTIFIER);
					
					while(!offerSuccess) {
						try {
							offerSuccess = bufferPopulatorWorkQueue.offerLast(END_OF_RESULTS_IDENTIFIER, 100, TimeUnit.MILLISECONDS);
						} catch (InterruptedException e) {
							//Don't care
						}
					}
				}
			}
		}
		finally {
			if(!cachingExplicitlyDisabled) {
				/*
				 * Whatever happens, exception or clean flow, we've finished with the lock now.
				 * Release it so that others can use it.
				 * Otherwise it will remained locked forever I think. 
				 */
				cacheModifyLock.unlock();
			}
		}
	}
	

	private void populateBuffer() {
						
		/*
		 * Loop 'forever'.
		 * Note: To end the thread, we return out of this method further down.
		 */
		while(true) {
			
			/*
			 * Peek for stuff from the work queue, waiting until we get something.
			 * 
			 * Note: we do the peek here, and the remove at the end; so that if a 
			 * problem occurs in this iteration:
			 * - we leave it on the queue for next time (Could this cause infinite loops?)
			 * - The buffer doesn't become prematurely early (as this is used part of the test
			 *   of whether to put directly to the fetchBuffer by runQuery)
			 */
			String fromWorkQueue;
			synchronized (bufferPopulatorWorkQueue) {
				fromWorkQueue = bufferPopulatorWorkQueue.peekFirst();
			}
			
			while(fromWorkQueue == null) {
				synchronized (bufferPopulatorWorkQueue) {
					fromWorkQueue = bufferPopulatorWorkQueue.peekFirst();
				}
				
				//Sleep so that someone else has a chance to synchronize before we loop again
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					//Don't care
				}
			}
			
			/*
			 * If we've hit the end of the results.
			 */
			if(fromWorkQueue.equals(END_OF_RESULTS_IDENTIFIER)) {
				
				putEndOfResultsIndicatorBatch();
				
				//Return - to shutdown this thread
				return;
			}
			/*
			 * Else something we need to populate into the fetchBuffer.
			 */
			else {
				Statement stmt = null;
				Connection c = null;
				GaianChildRSWrapper rsWrapper = null;
				
				try {
					c = getPooledLocalDerbyConnection();
					stmt = c.createStatement();
					
					/*
					 * Query the cache for the batch.
					 */
					ArrayList<DataValueDescriptor []> resultBatchList = new ArrayList<DataValueDescriptor[]>();
					
					ResultSet rs = stmt.executeQuery("SELECT " + getTableMetaData().getColumnNames() + " FROM " 
							+ getCacheSchemaAndTableName() + " WHERE CACHEID = " + fromWorkQueue);
					
					rsWrapper = new GaianChildRSWrapper(rs);
					
					DataValueDescriptor[] nextRow;
					
					//Create a new 'row'
					nextRow = new DataValueDescriptor[resultRowTemplate.length];
					
					//Fill the new row with empty copies of every DataValueDescriptor type in the rowTemplate
					for ( int j=0; j < resultRowTemplate.length; j++ ) {
						nextRow[j] = resultRowTemplate[j].getNewNull();
					}
					
					//While more rows from 'cache'
					while(rsWrapper.fetchNextRow(nextRow)) {
						
						//Add to batch
						resultBatchList.add(nextRow);
						
						/*
						 * Create a new 'row'
						 * AND
						 * Fill the new row with empty copies of every DataValueDescriptor type in the rowTemplate
						 * 
						 * Note: as we don't know how many we are likely to need, we do it one by one. I don't think this 
						 * is any more inefficient that making a batch to begin with.
						 */
						nextRow = new DataValueDescriptor[resultRowTemplate.length];
						
						for ( int j=0; j < resultRowTemplate.length; j++ ) {
							nextRow[j] = resultRowTemplate[j].getNewNull();
						}
					}
					
					//If no results - then something odd has happened
					if(resultBatchList.isEmpty()) {
						logger.logWarning(GDBMessages.DSWRAPPER_ICAREST_CACHE_READ_EMPTY, "A read request from " + fromWorkQueue + " returned no results when results were expected.");
					}
					else {
						DataValueDescriptor[][] resultBatch = resultBatchList.toArray(new DataValueDescriptor[][]{});
						
						/*
						 * Filter the data.
						 */
						DataValueDescriptor[][] rb = filterBatch(resultBatch);
						if ( null != rb ) resultBatch = rb;
						
						/*
						 * Put result batch onto fetchBuffer for derby to consume (through nextRow()).
						 * 
						 * Use looping offer, as this must happy.
						 */
						boolean offerSuccess = fetchBuffer.offerLast(resultBatch);
						
						while(!offerSuccess) {
							try {
								offerSuccess = fetchBuffer.offerLast(resultBatch, 100, TimeUnit.MILLISECONDS);
							} catch (InterruptedException e) {
								//Don't care
							}
						}
					}
					
					/*
					 * If we've got here without exception, then everything was done successfully.
					 * Now remove the element we peeked at from the front of the queue.
					 */
					synchronized (bufferPopulatorWorkQueue) {
						fromWorkQueue = bufferPopulatorWorkQueue.pollFirst();
					}
					
					while(fromWorkQueue == null) {
						synchronized (bufferPopulatorWorkQueue) {
							fromWorkQueue = bufferPopulatorWorkQueue.pollFirst();		
						}
						
						//Sleep so that someone else has a chance to synchronize before we loop again
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							//Don't care
						}
					}

				} catch (SQLSyntaxErrorException e) {
					// SQLSyntaxErrorException typically occurs if the cache table doesn't exist. We may have run out of disk space. Need to abort buffer population.
					logger.logException(GDBMessages.DSWRAPPER_ICAREST_CACHE_READ_ERROR, "Unable to fetch from the cache table. SQLSyntaxErrorException - Aborting rows buffer population.", e);
					putEndOfResultsIndicatorBatch();
					return;
				} catch (SQLException e) {
					logger.logException(GDBMessages.DSWRAPPER_ICAREST_CACHE_READ_ERROR, "Unable to fetch from the cache table. Warning: This may result in a partial result.", e);
				} finally {
					
					try {
						logger.logInfo("Closing select stmt isNull? " + (null==stmt) + ", and recycling its connection isActive? " + (null != c && !c.isClosed()) );
						
						if ( null != stmt ) stmt.close();
						if ( null != c && !c.isClosed() ) recyclePooledLocalDerbyConnection(c);
						if ( null != rsWrapper ) rsWrapper.close();
					}
					catch ( SQLException e ) {
						logger.logWarning(GDBMessages.DSWRAPPER_RECYCLE_CONNECTION_ERROR, "Unable to recycle connection after reading from cache table"); 
					}
				}
				
			}
		}
		
	}
	
	/**
	 * TODO
	 * @param resultBatch
	 */
	private DataValueDescriptor[][] filterBatch(DataValueDescriptor[][] resultBatch) {
		
		if(policyFilterDefined) {
			
			//If batch filtering available
			if(sqlResultFilterX != null) {
				//Note: use vtiArgs (the args passed into the VTI) as the datasourceid
				resultBatch = sqlResultFilterX.filterRowsBatch(this.vtiArgs, resultBatch);
			}
			//Else if single filtering available
			else if(sqlResultFilter != null) {
				
				//Create temp batch representing the records the user is allowed - this has max size resultBatch.length
				//Note: records are only added to this (and hence the index is only incremented) when a user is allowed to see them
				DataValueDescriptor[][] allowedBatch = new DataValueDescriptor[resultBatch.length][];
				int allowedBatchIndex = 0;
				
				for(int i = 0; i < resultBatch.length; i++) {
					if(sqlResultFilter.filterRow(resultBatch[i])) {
						allowedBatch[allowedBatchIndex] = resultBatch[i];
						allowedBatchIndex++;
					}
				}
				
				//Make resultBatch (which gets reported) a reduced copy of the allowed batch 
				resultBatch = Arrays.copyOf(allowedBatch, allowedBatchIndex);
			}
			//Else no filtering - and kind of error - as policyFilterDefined should not be true
		}
		//Else no filtering
		return resultBatch;
	}
	
	/**
	 * TODO
	 */
	private void putEndOfResultsIndicatorBatch() {
		/*
		 * Put an empty batch on the end of the fetchBuffer to indicate end of results
		 * - in case nextRow is still blocking on take.
		 * (This works in conjunction with queryRunning being false at this point - see nextRow())
		 * 
		 * Use looping offer, as this must happen.
		 */
		DataValueDescriptor[][] emptyBatch = new DataValueDescriptor[0][];
		
		boolean offerSuccess = fetchBuffer.offerLast(emptyBatch);
		
		while(!offerSuccess) {
			try {
				offerSuccess = fetchBuffer.offerLast(emptyBatch, 100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				//Don't care
			}
		}
	}
}
