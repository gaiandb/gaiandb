/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.searchapis;



import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Vector;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLInteger;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.siapi.common.ApplicationInfo;
import com.ibm.siapi.common.CollectionInfo;
import com.ibm.siapi.search.BaseQuery;
import com.ibm.siapi.search.Query;
import com.ibm.siapi.search.RemoteFederator;
import com.ibm.siapi.search.Result;
import com.ibm.siapi.search.ResultSet;
import com.ibm.siapi.search.SearchFactory;
import com.ibm.siapi.search.SearchService;
import com.ibm.siapi.search.Searchable;

public class SearchSIAPI {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "SearchSIAPI", 25 );

	public static void printTest() {
//		logger.logInfo("In Search");
	}

	// The Vector rows argument contains DataValueDescriptor[] elements, each of which is a derby database row.
	public static void retrieveDocumentReferences(Vector<DataValueDescriptor[]> rows, String hostname, String portNumber, String collections, 
			String queryString, int maxResults, String applicationName, String applicationPassword ) {
		
		try {
			// obtain the OmniFind specific SIAPI Search factory implementation
			SearchFactory searchFactory = (SearchFactory) Class.forName("com.ibm.es.api.search.RemoteSearchFactory").newInstance();
			ApplicationInfo appinfo = searchFactory.createApplicationInfo(applicationName);
			appinfo.setPassword(applicationPassword);
			int collectionSize=0;
			String[] collectionIDs=null;

			Query query = searchFactory.createQuery(queryString);
			query.setRequestedResultRange(0, maxResults);
			query.setQueryLanguage("en_US");
			query.setSpellCorrectionEnabled(true);
			query.setPredefinedResultsEnabled(true);
		    query.setReturnedAttribute(Query.RETURN_RESULT_FIELDS, true);
		    query.setReturnedAttribute(Query.RETURN_RESULT_URI, true);
		    query.setSortKey(BaseQuery.SORT_KEY_NONE);

			Properties config = new Properties();
			config.setProperty("hostname", hostname);
			config.setProperty("port", portNumber);
			config.setProperty("timeout", "60");
			config.setProperty("username", applicationName);
			config.setProperty("password", applicationPassword);

			// obtain the Search Service implementation
			SearchService searchService = searchFactory.getSearchService(config);
		    Searchable searchable = null;
//		    String collectionID = null;

		    logger.logInfo("Collections = " + collections);
		    //if there are no collections specified select all collections and construct a list of collectionIDs
		    if ( null == collections || "*".equals(collections) ) {
		    	
		    	RemoteFederator federator = searchService.getFederator(appinfo, appinfo.getId());
		    	if ( null == federator ) {
		    		logger.logWarning(GDBMessages.SIAPI_OMNIFIND_RESOLVE_ERROR, "Unable to resolve Omnifind Federator for app " + 
		    				applicationName + ", appID " + appinfo.getId() + 
		    				" - check collection name and id match, and check security for search app - returning no results");
		    		return;
		    	}
			    CollectionInfo[] CollectionInfos = federator.getCollectionInfos();
			    collectionIDs = new String[CollectionInfos.length];
			    
			    for (int i=0;i<CollectionInfos.length;i++)
			    	collectionIDs[i] = CollectionInfos[i].getID();
			    
			    collectionSize = CollectionInfos.length;
			    
		    } else {

				// Create an array of the list of collections from which search results are to be obtained
				String[] collectionNames = collections.split(",");
				collectionIDs = new String[collectionNames.length];
				for (int i=0;i<collectionNames.length;i++)
					collectionIDs[i] = CollectionIDfromName(searchService,appinfo,collectionNames[i]);
				
				collectionSize = collectionNames.length;
		    }

//		    String[] collectionIDs = collectionID.split(",");
//		    String[] collectionIDs = {"col_92810","col_56175","Graham","SSCatsa","col_57441"};

		    logger.logInfo(" CollectionSize= " + collectionSize +
		    		", CollectionIDs = " + Arrays.asList( collectionIDs ) );
		    
//		    String[] collectionIDs = {collectionID};
//		    String[] collectionIDs = new String[2];
//		    collectionIDs[0] = "col_57441";
//		    collectionIDs[1] = "SSCatsa";

		    for ( int collectionCount=0; collectionCount<collectionSize; collectionCount++ ) {

			    searchable = searchService.getSearchable(appinfo,collectionIDs[collectionCount].trim());
				ResultSet rs = searchable.search(query);

//				logger.logInfo("Estimated results: " + rs.getEstimatedNumberOfResults());
//				logger.logInfo("Available results: " + rs.getAvailableNumberOfResults());
				logger.logInfo("Evaluation time: " + rs.getQueryEvaluationTime());

	            if (rs != null) {
					// get the array of results from the ResultSet
					Result r[] = rs.getResults();
					if (r != null) {
						
						// walk the results list and print out the
						// document identifier
						int numResults = r.length;
						logger.logInfo("Number of results: " + numResults  );
						
						for (int k = 0; k < numResults; k++) {
//		       				get the entry element
		               		String uri = r[k].getDocumentID();

//		               		logger.logInfo("Description = " +r[k].getDescription());
		               		if ( null == uri ) continue;

		       				//documentPath = (String) documentPath.subSequence(0,documentPath.length()-1);
		               		
		               		// DRV 09/12/10 - Removing:
		               		// 1) decoding of URIs and hence, 2) option to hash on the encoded or decoded URIs 
//		       				String documentPath = URLDecoder.decode( uri, Charset.defaultCharset().name() );
//		               		int id = hashDecodedPaths ? documentPath.hashCode() : uri.hashCode();
		       				
		       				int id = uri.hashCode();
		       				
		       				logger.logInfo("Adding row with docHashID: " + id + ", docURI: " + uri);
//		       				logger.logInfo("Other row info description: " + r[k].getDescription());
		       				
		       				rows.add( new DataValueDescriptor[] { new SQLInteger(id), new SQLChar(uri), new SQLChar(r[k].getDescription()) } );
						}
					}
	            }
			}
		} catch (Exception e) {
			logger.logException(GDBMessages.SIAPI_EE_DOC_SEARCH_RESOLVE_ERROR, "Unable to resolve Omnifind EE document search, cause: ", e);
		}
	}

	protected static void printUsage() {
//		logger.logInfo("Search <hostname> <port> <index> <query> [Local XSL File]");
	}

	protected static byte[] streamToByteArray(InputStream stream) throws IOException {

	    // Handle null
	    if(stream == null) {
	       return null;
	    }

	    BufferedInputStream bufStream = new BufferedInputStream(stream);

	    ByteArrayOutputStream outStream = new ByteArrayOutputStream();

	    // Read in 4K chunks - this could be increased to improve performance, but
	    // 4K seems to be a generally accepted size to use.
	    byte[] buffer = new byte[4096];
	    int len = 0;
	    while((len = bufStream.read(buffer, 0, 4096)) > 0) {
	       outStream.write(buffer, 0, len);
	    }

	    bufStream.close();
	    outStream.close();

	    return outStream.toByteArray();
	}

	public static String CollectionIDfromName(SearchService searchService,ApplicationInfo appinfo,String collectionName){
		try {
			// obtain the list of collection names and id's that the current user can access
			RemoteFederator federator = searchService.getFederator(appinfo, appinfo.getId());
//	      	logger.logInfo("Appinfo_id = " + appinfo.getId());
			CollectionInfo[] CollectionInfos = federator.getCollectionInfos();
			
			for (int i=0; i<CollectionInfos.length; i++) {
				if ( collectionName.equals(CollectionInfos[i].getLabel()) ){
					collectionName = CollectionInfos[i].getID();
					return collectionName;
				}
			}
		} catch(Exception e){
		}
		
		return collectionName;
	}
}

