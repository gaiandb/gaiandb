/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.net.ConnectException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.vti.Pushable;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.GaianDBConfigProcedures;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.mongodb.MongoMessages;
import com.ibm.gaiandb.mongodb.MongoConnectionFactory;
import com.ibm.gaiandb.mongodb.MongoConnectionParams;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * This class implements a Virtual Table Interface allowing GaianDB 
 * to retrieve data from MongoDB databases.
 * 
 * @author Paul Stone
 */
public class MongoDB extends AbstractVTI implements Pushable {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";
	
	private static final Logger logger = new Logger( "MongoDB", 10 );
	
	public enum CombinationOperator { OR, AND };

	//define string constants which map to the configuration parameters
	static final String PROP_ADDRESS = "address";
	static final String PROP_PORT = "port";
	static final String PROP_DB_NAME = "db";
	static final String PROP_COLLECTION_NAME = "collection";
	static final String PROP_USER = "user";
	static final String PROP_PASSWORD = "password";
	
	// constant representing an "unconstrained" mongoDB query - will match any document in a mongo collection.
	private static final BasicDBObject QUERY_ANYTHING = new BasicDBObject();
	
	// instanceMongoQuery is a handle to the mongoDB collection, queries can be executed 
	// against this handle to retrieve data.
	DBCollection instanceMongoCollection;
	// instanceMongoQuery is used to hold the qualifier values in a format suitable for querying mongo.
	// an empty object matches all documents in mongo.
	BasicDBObject instanceMongoQuery;
	// mongoAllColumns is used to hold the field names to return for the whole logical table. Note that the "id" field is always returned
	// from mongo for each document.
	BasicDBObject mongoAllColumns;
	// mongoQueryColumns is used to hold the field names to return from the latest specific mongo query. .
	BasicDBObject mongoQueryColumns;
	// mongoResults references the results of the latest mongoDB query. The result data can 
	// be retrieved from this object.
	DBCursor mongoResults;
	
	/**
	 * This method creates a connection to the appropriate mongo process/database/collection 
	 * according to the parameters in the Gaian Configuration file. 
	 * @param vtiArgs - contains the Datasource "_ARGS" field from the config file.
	 * @throws This will throw an exception if we are unable to connect to the specified, 
	 * 		   configured Mongo database process.
	 */
	public MongoDB(String vtiArgs) throws Exception {
		// The super class constructor sets up the VTI parameters from the configuration file.
		super(vtiArgs, "MongoDBVTI");
		
		// Get the connection details from the configuration.
		MongoConnectionParams connDetails = 
			new MongoConnectionParams (
				getVTIProperty (PROP_ADDRESS),
				Integer.parseInt(getVTIProperty (PROP_PORT)),
				getVTIPropertyWithReplacements (PROP_DB_NAME),
				getVTIPropertyWithReplacements (PROP_COLLECTION_NAME),
				getVTIPropertyNullable (PROP_USER),
				getVTIPropertyNullable (PROP_PASSWORD) );

		// Connect to the mongo collection, reference the collection as an instance variable 
		// so other methods - particularly executeAsFastPath can use it.
		instanceMongoCollection = MongoConnectionFactory.getMongoCollection(connDetails);

		if (instanceMongoCollection == null) throw new ConnectException(MongoMessages.DSWRAPPER_MONGODB_COLLECTION_ACCESS_ERROR);

		//work out which fields we should be extracting. Limit the future queries to these fields.
		GaianResultSetMetaData rowDescription;
		try {
			rowDescription = getMetaData();
		} catch (SQLException e1) {
			logger.logException(MongoMessages.DSWRAPPER_MONGODB_META_DATA_ERROR,MongoMessages.DSWRAPPER_MONGODB_META_DATA_ERROR, e1);
			return;
		}
		mongoAllColumns = new BasicDBObject();

		// Go through the items defined in the config, restrict the query to these rows only.
		for ( int columnId = 0; columnId < rowDescription.getColumnCount(); columnId++){
			String fieldName = rowDescription.getColumnName(columnId+1);
			mongoAllColumns.put(fieldName, 1);
		}
		
		// Assume that the query will return all columns - this will be changed if pushProjection is called.
		mongoQueryColumns = mongoAllColumns;

	}
	
	/**
	 * This method will execute a query against the connected Mongo Process.
	 * By this stage we should have connected to the Mongo database and collection and
	 * have processed any qualifiers and projections.
	 * Any qualifiers passed in "setQualifiers" and any column projections passed in "pushProjection"
	 * are used to constrain the query. 
	 * The result of the query is held as an instance variable for further access using the nextRow() method.
	 * @throws SQLException if there is no connection to a mongo process or the query fails.
	 * @return "true" will always be returned unless an exception is raised.
	 */
	@Override
	public boolean executeAsFastPath() throws SQLException {
		if (instanceMongoCollection == null) {
			// We don't have a valid Mongo Collection so we can't execute the query.
			throw new SQLException(MongoMessages.DSWRAPPER_MONGODB_NOT_CONNECTED);
		}
		
		//Initialise these in case we have not been passed qualifiers or projected columns
		BasicDBObject mongoQuery = instanceMongoQuery;
		if (mongoQuery == null) mongoQuery = new BasicDBObject();

		if (mongoQueryColumns == null) {
			//Call mongo to find any document matching our query
			mongoResults = instanceMongoCollection.find(mongoQuery);
		} else {
			//Call mongo to find any document matching our query
			mongoResults = instanceMongoCollection.find(mongoQuery, mongoQueryColumns);
		}

		if (mongoResults == null) {
			// for some reason the 
			throw new SQLException(MongoMessages.DSWRAPPER_RESULTSET_NOT_CONNECTED);
		}
		
		mongoResults.batchSize(1000); //configure mongo client to pull back 1000 results at a time.

		return true;
	}
	
	/**
	 * This method maps from a derby Qualifier operator to a mongoDB operator string.
	 * @param qualifierOperator - a derby format operator.
	 * @return String - the equivalent mongoDB text to the qualifierOperator.
	 */
	private String mongoOperatorLookup (int qualifierOperator) {
		switch (qualifierOperator) {
		case (Orderable.ORDER_OP_LESSTHAN): return ("$lt");
		case (Orderable.ORDER_OP_LESSOREQUALS):return ("$lte");
		case (Orderable.ORDER_OP_GREATERTHAN):return ("$gt");
		case (Orderable.ORDER_OP_GREATEROREQUALS):return ("$gte");
		default: 
			logger.logImportant("MongoDB.mongoOperatorLookup - qualifierOperator: " + qualifierOperator + " is not known.");
			throw new IllegalArgumentException("qualiferOperator: " + qualifierOperator);
		}
	}
	
	/**
	 * This method adds an operator to the mongo query to enforce the derby qualifier specified.
	 * @param mongoConditions - Object representing the conditions of a mongoDB query. 
	 * 						    This will be modified to enforce the qualifier condition
	 * @param qual - A derby format qualifier, holding a condition that should be added to the query. 
	 */	
	public void addMongoOperator(BasicDBObject mongoConditions, Qualifier qual) {
		try {
			// work out the two component parts - the field and the value being compared
			String mongoFieldName = getMetaData().getColumnName(qual.getColumnId() + 1);
			Object mongoValue = qual.getOrderable().getObject();
			
			if (qual.getOperator()== Orderable.ORDER_OP_EQUALS){
				//equals operator is a special case
				if (qual.negateCompareResult()){
					mongoConditions.append(mongoFieldName, new BasicDBObject ("$ne", mongoValue)); //$ne is the mongo "not equals" operator, so this is "field != value"
				} else {
					mongoConditions.append(mongoFieldName, mongoValue); //add a condition that field = value
				}
			} else {
				String mongoOperatorText = mongoOperatorLookup(qual.getOperator());
				
				if (qual.negateCompareResult()){
					// add a condition comparing the field and value with the negated operator
					mongoConditions.append(mongoFieldName, new BasicDBObject("$not", new BasicDBObject (mongoOperatorText, mongoValue)));
				} else {
					// add a condition comparing the field and value with the operator
					mongoConditions.append(mongoFieldName, new BasicDBObject (mongoOperatorText, mongoValue));
				}
				
			}
	
		} catch (SQLException e) {
			// This happens if we cannot resolve the qualifier metadata.
			// log the exception and carry on without this qualifier.
			logger.logException(MongoMessages.DSWRAPPER_MONGODB_QUALIFIER_META_DATA_ERROR, "Meta Data resolution failed for " + getPrefix(),e);
		} catch (StandardException e) {
			// This happens when the qualifier.getOrderable throws an error.
			// log the exception and carry on without this qualifier.
			logger.logException(MongoMessages.DSWRAPPER_MONGODB_QUALIFIER_ACCESS_ERROR, "Qualifier error for: " + qual.toString(),e);
		}
	}
	
	/**
	 * This method adds an operator to the mongo query to enforce the derby qualifiers specified.
	 * @param mongoConditions - Object representing the conditions of a mongoDB query. 
	 * 						    This will be modified to enforce the qualRow conditions
	 * @param qualRow - an array of derby database qualifiers, holding a set of conditions 
	 * 		            that should be added to the query. 
	 */	
	public void addMongoOperators (BasicDBList mongoConditions, Qualifier qualRow[]){
		if (qualRow != null) {
			for (Qualifier qual : qualRow) {
				BasicDBObject insideOperator = new BasicDBObject();
				addMongoOperator(insideOperator, qual);
				mongoConditions.add(insideOperator);
			}
		}
	}
	
	/**
	 * This method adds an operator to the mongo query to enforce the derby qualifiers specified.
	 * @param query - Object representing the conditions of a mongoDB query. 
	 * 				  This will be modified to enforce the qualRow conditions
	 * @param qualRow - an array of derby database qualifiers, holding a set of conditions 
	 * 		            that should be added to the query. 
	 * @param operator - indicates how the qualRow conditions should be combined with the query - "OR" or "AND".
	 */	
	public void addMongoOperatorRow(BasicDBObject query, Qualifier qualRow[], CombinationOperator operator){
		if (qualRow == null || qualRow.length == 0){
			// Don't include any entry for this row.
		} else if (qualRow.length == 1){
			// just include the single entry
			addMongoOperator(query, qualRow[0]);		
		} else {
			// we have > 1 qualifier so they need including in a list, prefixed by the
			// necessary operator.
			String combinationText;
			switch (operator){
				case OR: combinationText = "$or";				
					break;
				case AND: combinationText = "$and";
					break;
				default: // throw exception
					combinationText = "$invalid";
			}
		
			// construct a separate sub-list of conditions to hold the qualifiers. 
			BasicDBList insideOperatorList = new BasicDBList();
			addMongoOperators(insideOperatorList, qualRow);
			query.append(combinationText,insideOperatorList);
		}
	}

	/**
	 * This method takes the derby qualifiers specified and determines how to apply the same conditions to
	 * a mongo query so that the correct, matching mongo documents are returned. The generated mongo query 
	 * is held as an instance variable in "instanceMongoQuery" for when the query is executed.
	 * @param vtie - I handle to VTI Environment parameters - not used for this VTI.
	 * @param qualMatrix - a two dimensional array of qualifiers determining the conditions for matching 
	 *                     query results.  This is held in Conjunctive Normal Form (CNF) and is described 
	 *                     the java doc for the org.apache.derby.iapi.store.access.Qualifier class
     */	
	@Override
	public void setQualifiers(VTIEnvironment vtie, Qualifier[][] qualMatrix){
		
		// a local variable to build the mongo query structure.
		BasicDBObject mongoQueryOperators = new BasicDBObject();
		
		if (qualMatrix != null) {
			// The first slot of the 2 dimensional qual array is a list of "and" conditions.
			// The remaining slots of the 2 dimensional qual array are list of "or" conditions.
			// all slots are combined by "and"ing them together.
			
			// represent mongo conditions as {{row-1-conditions},{row-2-conditions},...,{row-n-conditions}}
			for (int index = 0; index < qualMatrix.length; index++){ 
				Qualifier qualRow[] = qualMatrix[index];
				
				// work out what logical operator is used to join multiple conditions. This is determined by derby
				// see derby javadoc for qualifiers.
				CombinationOperator operator;
				if (index ==0){
					operator = CombinationOperator.AND; // the first row of qualifiers are combined by "and" conditions
				} else {
					operator = CombinationOperator.OR; // other qualifier rows are combined by "or" conditions
				}
				addMongoOperatorRow(mongoQueryOperators,qualRow,operator);
			}
		}
		instanceMongoQuery = mongoQueryOperators;
		
		logger.logInfo("MongoVTI - query will use qualifiers: "+ mongoQueryOperators);
	}
	
	/**
	 * This method takes a mongo document resulting from a query and converts it into a row of data suitable to be 
	 * returned to Derby. 
	 * The datasource meta data is used to determine how fields in the mongo document map to derby columns.
	 * This method performs type casting from mongo to derby data types.
	 * Fields that are unsuccessfully parsed are returned as null in the derbyRow.
	 * @param mongoDoc - A mongo Document returned froma query
	 * @param derbyRow - An array of DataValueDescriptors, used to pass data results to Derby. This is updated.
	 * @return boolean - indicates the success of the document translation.
     */	
	private boolean parseBSONMongoDocument (DBObject mongoDoc, DataValueDescriptor[] derbyRow ) {
		
		//get meta data to determine which fields and columns we expect
		GaianResultSetMetaData rowDescription;
		try {
			rowDescription = getMetaData();
		} catch (SQLException e1) {

			logger.logException(MongoMessages.DSWRAPPER_MONGODB_META_DATA_ERROR, "MongoDB.parseBSONMongoDocument Failed to get table meta data", e1);
			return false;
		}
		// Go through the items in the derby row and see if we have a matching field in the result row from mongo.
		for ( int columnId = 0; columnId < rowDescription.getColumnCount(); columnId++){
			String fieldName = rowDescription.getColumnName(columnId+1); //TBD this call has a "log" statement - should optimise to call it infrequently
			//find the field in the mongo result row
			Object mongoField = mongoDoc.get(fieldName);
			if(mongoField != null) {
				try {
					// Set the value of the derby row according to the correct data type.
					switch (rowDescription.getColumnType(columnId+1)) {
					case java.sql.Types.VARCHAR:
						
						String value = null;
						if (mongoField instanceof java.lang.String) {
							value = (String)mongoField;
						} else if (mongoField instanceof org.bson.types.ObjectId) {
							value = ((org.bson.types.ObjectId)mongoField).toStringMongod();
						} else if (mongoField instanceof com.mongodb.BasicDBObject) {
							value = ((com.mongodb.BasicDBObject)mongoField).toString();
						} else if (mongoField instanceof com.mongodb.BasicDBList) {
							value = ((com.mongodb.BasicDBList)mongoField).toString();
						} else	if (mongoField instanceof org.bson.types.BSONTimestamp) {
							value = ((org.bson.types.BSONTimestamp)mongoField).toString();					
						} else {
							logger.logWarning(MongoMessages.DSWRAPPER_MONGODB_INCOMPATIBLE_TYPE_ERROR, 
									"Could not map to VARCHAR, field named "+ fieldName +" of type: "+ mongoField.getClass() );
						}
						derbyRow[columnId].setValue( value );						
						break;
					case java.sql.Types.CLOB:
						
						String valueCLOB = null;
						if (mongoField instanceof java.lang.String) {
							valueCLOB = (String)mongoField;
						} else if (mongoField instanceof org.bson.types.ObjectId) {
							valueCLOB = ((org.bson.types.ObjectId)mongoField).toStringMongod();
						} else if (mongoField instanceof com.mongodb.BasicDBObject) {
							valueCLOB = ((com.mongodb.BasicDBObject)mongoField).toString();
						} else if (mongoField instanceof com.mongodb.BasicDBList) {
							valueCLOB = ((com.mongodb.BasicDBList)mongoField).toString();
						} else if (mongoField instanceof org.bson.types.BSONTimestamp) {
							value = ((org.bson.types.BSONTimestamp)mongoField).toString();					
						} else {
							logger.logWarning(MongoMessages.DSWRAPPER_MONGODB_INCOMPATIBLE_TYPE_ERROR, 
									"Could not map to CLOB, field named "+ fieldName +" of type: "+ mongoField.getClass() );

						}
						derbyRow[columnId].setValue( valueCLOB );						
						break;
					case java.sql.Types.INTEGER:
						int valuei;
						if (mongoField instanceof Integer) {
							valuei = ((Integer)mongoField).intValue();
						} else if (mongoField instanceof org.bson.types.BSONTimestamp) {
							valuei = ((org.bson.types.BSONTimestamp)mongoField).getTime(); // sql.timestamp is in milliseconds, the mongo timestamp is in seconds.					
						} else {
							logger.logWarning(MongoMessages.DSWRAPPER_MONGODB_INCOMPATIBLE_TYPE_ERROR, 
									"Could not map to INTEGER, field named "+ fieldName +" of type: "+ mongoField.getClass() );
							valuei = - Integer.MIN_VALUE;
						}

						derbyRow[columnId].setValue( valuei );						
						break;	
					case java.sql.Types.DOUBLE:
						double valued = ((Double)mongoField).doubleValue();
						derbyRow[columnId].setValue( valued );						
						break;
					case java.sql.Types.BOOLEAN:
						boolean valueb = ((Boolean)mongoField).booleanValue();
						derbyRow[columnId].setValue( valueb );						
						break;
					case java.sql.Types.DATE:
						java.sql.Date valueDate = new java.sql.Date(((Date)mongoField).getTime());
						derbyRow[columnId].setValue( valueDate );
						break;
					case java.sql.Types.TIMESTAMP:
						Timestamp valueTS = null;
						if (mongoField instanceof org.bson.types.BSONTimestamp) {
							valueTS = new Timestamp (((org.bson.types.BSONTimestamp)mongoField).getTime()*1000); // sql.timestamp is in milliseconds, the mongo timestamp is in seconds.					
						} else {
							logger.logWarning(MongoMessages.DSWRAPPER_MONGODB_INCOMPATIBLE_TYPE_ERROR, 
									"Could not map to TIMESTAMP, field named "+ fieldName +" of type: "+ mongoField.getClass() );
						}
						derbyRow[columnId].setValue( valueTS );
						break;
					default:
						//This is a type we are not expecting.
						logger.logWarning(MongoMessages.DSWRAPPER_MONGODB_INCOMPATIBLE_TYPE_ERROR, 
								"Could not map a field named "+ fieldName +" of type: "+ 
								mongoField.getClass()+", to type "+rowDescription.getColumnType(columnId+1) );
						break;				
					}
				} catch (StandardException e) {
					// This error is thrown when we unable to set a value in the derby row.
					e.printStackTrace();
					logger.logException(MongoMessages.DSWRAPPER_MONGODB_VALUE_CONVERSION_ERROR, 
							"Could not convert result to Derby Type. Field: " + fieldName +", Value: " + mongoField.toString(), e);
				} catch (Exception e){
					// This error is thrown when we unable to set a value in the derby row.
					e.printStackTrace();
					logger.logException(MongoMessages.DSWRAPPER_MONGODB_VALUE_CONVERSION_ERROR, 
							"Unknown Error converting result to Derby Type. Field: " + fieldName +", Value: " + mongoField.toString(), e);
					
				}
			}
			
		}
		
		return true;
	}

	/**
	 * This method returns a result row from the executed query. 
	 * @param arg0 - A data structure which is populated with the result row.
	 * @return a success status flag (see org.apache.derby.vti.IFastPath javadoc for valid values).
	 */
	@Override
	public int nextRow(DataValueDescriptor[] arg0) throws StandardException,
			SQLException {
		// parsedValidRow is the flag indicating success - initialise to false.
		boolean parsedValidRow = false;
		if (mongoResults != null) {
			while (mongoResults.hasNext()&& !parsedValidRow) {
				//Parse the Mongo result into a DataValueDescriptor format for Derby
				DBObject resultRow = mongoResults.next();
				parsedValidRow = parseBSONMongoDocument(resultRow, arg0);
			}
		}
			
		if (parsedValidRow){
			return GOT_ROW;
		} else {
			return SCAN_COMPLETED;	
		}
	}

	/**
	 * This method returns the number of rows which have resulted from the last  executed query
	 * @return int - count of satisfying rows.
	 */
		@Override
	public int getRowCount() throws Exception {
		if (mongoResults != null){
			return mongoResults.count();
		} else {
			return 0;
		}
	}

	/**
	 * This method returns an estimate of the row instantiation cost - used by the derby query 
	 * optimiser.
	 * Currently returns a fixed value - 100.0!
	 * @return double - returns 100.0.
	 */
	@Override
	public double getEstimatedCostPerInstantiation(VTIEnvironment arg0)
			throws SQLException {
		// This is just a simple implementation - could be improved with real statistics.
		return 100.0d;
	}

	/**
	 * This method returns an estimate of the rows that would be fetched - used by the derby query 
	 * optimiser.
	 * Currently returns a fixed value - 1.0!
	 * @return double - returns 1.0.
	 */
	@Override
	public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
		// This is just a simple implementation - could be improved with real statistics.
		return 1.0d;
	}

	/**
	 * This method returns an estimate of the rows that would be fetched - used by the derby query 
	 * planner.
	 * Currently returns a fixed value - false!
	 * @return boolean - returns false.
	 */	@Override
	public boolean supportsMultipleInstantiations(VTIEnvironment arg0)
			throws SQLException {
		// This is just a simple implementation - could be improved.
		return false;
	}

	 // This method resets the state of all VTI instance variables apart from the
	 // instanceMongoCollection, which is kept open to allow the VTI to be reused
	 // for other queries to the same MongoDB collection.
	 @Override
	 public boolean reinitialise() {
		 instanceMongoQuery = null;
		 // Assume that the next query will return all columns - this will be changed if pushProjection is called.
	     mongoQueryColumns = mongoAllColumns;
	     if (mongoResults !=null) {mongoResults.close();} //important to close the database cursor to free resources.
	     mongoResults = null;
		 return true;
	 }
		
	@Override
	public void close() throws SQLException {
		super.close();
		reinitialise();// closes all the local resources apart from the instanceMongoCollection.
		MongoConnectionFactory.closeMongoCollection(instanceMongoCollection); 
	}

	/**
	 * This method is called to inform the MongoVTI which columns that must be returned by the active query.
	 * This method is called only during the runtime execution of the VTI, after it has been constructed and before the executeQuery() method is called.
	 * The column identifiers contained in projectedColumns map to the columns described by the VTI's PreparedStatement's ResultSetMetaData.
	 * The JDBC column numbering scheme (1 based) is used for projectedColumns.
	 * 
	 * The column fields passed are used to reduce the data retrieved from Mongo in the subsequent call 
	 * to executeAsFastPath().
	 * 
	 * @throws java.sql.SQLException - Error processing the request.
	 * @return false indicating that we do not implement getXXX() methods.
	 * NOTE! ==> The return value is ignored by Derby if the VTI implements IFastPath (because it will by-pass the ResultSet getXXX() methods).
	 */

	@Override
	public boolean pushProjection(VTIEnvironment vtiEnvironment, int[] projectedColumns) throws SQLException{
		//work out which fields we should be extracting. Limit the future queries to these fields.
		GaianResultSetMetaData rowDescription = getMetaData();
		mongoQueryColumns = new BasicDBObject();

		// Go through the items defined in the config, restrict the query to these rows only.
		for ( int columnIndex = 0; columnIndex < projectedColumns.length; columnIndex++){
			int columnID = projectedColumns[columnIndex];
			String fieldName = rowDescription.getColumnName(columnID);
			mongoQueryColumns.put(fieldName, 1);
		}		
		logger.logInfo("MongoVTI - query will fetch column: "+ mongoQueryColumns);
		
		return false;
	}

	/**
	 * This method analyses the result document from Mongo and determines a suitable  
	 * initial Logical Table definition from it.
	 * 
	 * @param resultDoc - the mongo document that has been retrieved from the collection.
	 * @return a String representing a Logical Table definition appropriate to the resultDoc.
	 */
	// 
	// 
	private static String generateLTDefFromMongoDocument (DBObject resultDoc){
		StringBuilder ltDef = new StringBuilder();
		for (String fieldName : resultDoc.keySet()){
			// The field gives us the column name
			Object mongoField = resultDoc.get(fieldName);
			if(mongoField != null) {
				if (mongoField instanceof java.lang.String){
					ltDef = ltDef.append(fieldName).append(" VARCHAR(255), ");
				} else if (mongoField instanceof java.lang.Integer){
					ltDef = ltDef.append(fieldName).append(" INTEGER, ");
				} else if (mongoField instanceof java.lang.Double){
					ltDef = ltDef.append(fieldName).append(" DOUBLE, ");
				} else if (mongoField instanceof java.lang.Boolean){
					ltDef = ltDef.append(fieldName).append(" BOOLEAN, ");
				} else if (mongoField instanceof java.util.Date){
					ltDef = ltDef.append(fieldName).append(" DATE, ");
				} else if (mongoField instanceof org.bson.types.BSONTimestamp){
					ltDef = ltDef.append(fieldName).append(" TIMESTAMP, ");
				} else if(mongoField instanceof org.bson.types.ObjectId ||
						mongoField instanceof com.mongodb.BasicDBObject ||
						mongoField instanceof com.mongodb.BasicDBList) {
					ltDef = ltDef.append(fieldName).append(" VARCHAR(255), ");
				}

			}
		}
		//remove a trailing ", "..
		int defLength = ltDef.length();
		if (defLength>2){
			ltDef = ltDef.delete(defLength-2, defLength-1);
		}
		return ltDef.toString();
	}
	/**
	 * This method sends a request to a mongoDB instance, reads the data returned by the 
	 * web service, and determines which columns  can represent the 
	 * associated logical table. It also writes the necessary properties into the 
	 * gaian property file in order to query the logical table later on.
	 * 
	 * @param ltName - Name of the generated logical table.
	 * @param url - Url accessing the mongo process. 
	 * 		Expected format: {user}:{password}@{MongoURL}:{Port}/{Database}/{Collection}
	 * 		The {user}:{password}@ portion is optional, and implies that authentication is required. 			 
	 * @param fields - 	an optional list of fields to be extracted for the logical table.
	 * @throws Exception on some sub-method failure.
	 */
	public static synchronized void setLogicalTableForMongoDB( String ltName, String url, String fields) throws Exception {

		ltName = ltName.toUpperCase();
		
		logger.logInfo("Obtaining tableDef for mongo process: " + url);
		
		MongoConnectionParams connDetails = new MongoConnectionParams (url);

		DBCollection mongoCollection = MongoConnectionFactory.getMongoCollection(connDetails);

		// If we get this far without exception then the connection parameters must all be valid. Now work out the logical table definition.
		
		String ltDef ="";
		
		DBObject mongoResult;
		if (fields == null || fields =="") {
			mongoResult = mongoCollection.findOne(QUERY_ANYTHING); //retrieves just the first row
			
		} else {
			 BasicDBObject keys = new BasicDBObject();
			 for (String field : fields.split(",")){
				 keys.put(field.trim(), 1);
			 }
			mongoResult = mongoCollection.findOne(QUERY_ANYTHING, keys ); //retrieves just the first row matching the "keys"
			
		}

		if (mongoResult != null) {
			ltDef = generateLTDefFromMongoDocument(mongoResult);
		}
				
		// Logical Table properties
		Map<String, String> ltProperties = GaianDBConfigProcedures.prepareLogicalTable( ltName, ltDef, "" );
		
		// Data Source Definition properties
		ltProperties.put(ltName+"_DS0_ARGS",ltName+"conf, "+connDetails.getDatabaseName()+", "+connDetails.getCollectionName()); 
		ltProperties.put(ltName+"_DS0_VTI",MongoDB.class.getName()); 
		
		// VTI definition properties
		String vtiPropertiesPrefix = MongoDB.class.getSimpleName() + "." + ltName + "conf.";

		ltProperties.put(vtiPropertiesPrefix + "schema", ltDef);
		ltProperties.put(vtiPropertiesPrefix + PROP_ADDRESS, connDetails.getHostAddress());
		ltProperties.put(vtiPropertiesPrefix + PROP_PORT, connDetails.getHostPort().toString());
		ltProperties.put(vtiPropertiesPrefix + PROP_DB_NAME, "$0"); // $0 is a replacement token to get the config to the DS0_ARGS field
		ltProperties.put(vtiPropertiesPrefix + PROP_COLLECTION_NAME, "$1");// $0 is a replacement token to get the config to the DS0_ARGS field

		// Note that if user or password is null, any existing key will be deleted, which is what we want to happen.
		ltProperties.put(vtiPropertiesPrefix + PROP_USER, connDetails.getUserName());
		ltProperties.put(vtiPropertiesPrefix + PROP_PASSWORD, connDetails.getPassword());

		// Now actually update and save the configuration.
		GaianDBConfigProcedures.setConfigProperties(ltProperties);
//		GaianDBConfigProcedures.persistAndApplyConfigUpdates(ltProperties);
		
		// The following call ensures that the configuration is fully loaded 
		// and we are ready to query the table. Without this, an immediate call 
		// to query the table can fail.
		DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
		
		MongoConnectionFactory.closeMongoCollection(mongoCollection);
	}
	

}
