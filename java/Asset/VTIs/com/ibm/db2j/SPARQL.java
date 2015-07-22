/* Licensed Materials - Property of IBM
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.VTIEnvironment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.ibm.gaiandb.Logger;

/**
 * SPARQL web service - uses a query defined in a .sparql file, combined with
 * some properties in the gaindb_config.properties file to query a SPARQL
 * endpoint and return the results in a tabular format.
 * 
 * @author Ed Jellard
 * 
 */
public class SPARQL extends AbstractVTI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private static final Logger logger = new Logger( "SPARQL", 20 );

	private enum SparqlType {STR, IRI, NUM};
	
	static final String PROP_SPARQL_TYPES = "sparqltypes";
	//The key field here is the name of the SPARQL variable and the value is type of the variable
	HashMap<String, SparqlType> sparqlTypes;

	private StringBuilder filterText = new StringBuilder();
	private StringBuilder whereText = new StringBuilder();
	private int currentRow = 0;
//	private Document doc;
	private NodeList results;
	
	private static final int MAX_ROWS_TO_CACHE_BEFORE_SPILL_TO_DISK = 100;

	private void populateSparqlTypes() {
System.out.println("SPARQL.populateSparqlTypes");
		String sparqlTypesConfig = getVTIPropertyNullable(PROP_SPARQL_TYPES);
		sparqlTypes = new HashMap<String, SparqlType>();
		if (sparqlTypesConfig != null){
			String[] types = sparqlTypesConfig.split(",");
			for ( int i=0; i<types.length; i++ ){
				String[] tokens = types[i].trim().split(" ");
				if (tokens.length == 2){

					try{
						sparqlTypes.put(tokens[0], SparqlType.valueOf(tokens[1]));
					} catch (IllegalArgumentException e){
						logger.logWarning("SPARQL error", "Illegal SPARQL Type defined for "+ super.getPrefix()+", " + tokens.toString() );}
				}
				else {
				logger.logWarning("SPARQL error", "Invalid SPARQL Type definition for "+ super.getPrefix()+", " + tokens.toString() );
				}
			}
		}
	}	

	public SPARQL(String constructor) throws Exception {
		super(constructor, "sparql");
System.out.println("SPARQL.SPARQL - " + constructor);
		populateSparqlTypes();
	}
	
	public int nextRow(DataValueDescriptor[] row) throws StandardException, SQLException {
System.out.println("SPARQL.nextRow");
//		logger.logInfo("nextRow called for SPARQL Instance: " +this.hashCode());
		if (isCached()) {
			return nextRowFromCache(row);
		}
		
		//results are not cached, go through and cache them all
		 ArrayList<DataValueDescriptor[]> cachedResultRows = new ArrayList<DataValueDescriptor[]>( 10 );

		for (int rowNo =0; rowNo<results.getLength(); rowNo++){
			DataValueDescriptor[] cachedRow = new DataValueDescriptor[row.length];
			for (int i=0; i<row.length; i++) {
				cachedRow[i] = row[i].getNewNull();
			}
			Element el = (Element) results.item(rowNo);
			NodeList inl = el.getElementsByTagName("binding");
			for (int j = 0; j < inl.getLength(); j++) {
				Element iel = (Element) inl.item(j);
				NodeList iiel = iel.getChildNodes();
				for (int k = 0; k < iiel.getLength(); k++) {
					if (iiel.item(k).getNodeType() == Element.ELEMENT_NODE) {
						Element child = (Element) iiel.item(k);
						String variable = iel.getAttribute("name");
						int colPosition = getMetaData().getColumnPosition(variable) - 1;
						cachedRow[colPosition].setValue(child.getTextContent());
						break;
					}
				}

			}
			cachedResultRows.add( cachedRow );
			
			if (cachedResultRows.size() == MAX_ROWS_TO_CACHE_BEFORE_SPILL_TO_DISK) {
				cacheRows( cachedResultRows.toArray( new DataValueDescriptor[0][] ), getMetaData().getColumnCount() );
				cachedResultRows.clear();
			}

		}
		
		if (cachedResultRows.size() > 0) {
			cacheRows( cachedResultRows.toArray( new DataValueDescriptor[0][] ), getMetaData().getColumnCount() );
			cachedResultRows.clear();
		}

		
		isCached(whereText.toString()); //resets the iscached flag to true if cache was successful.
//		NodeList results = doc.getElementsByTagName("result");
		return nextRowFromCache(row);
	}

	public int getRowCount() throws Exception {
System.out.println("SPARQL.getRowCount");
		return results.getLength();
	}
	
	public boolean isBeforeFirst() {
System.out.println("SPARQL.isBeforeFirst");
		return 0 == currentRow;
	}
	
	public boolean executeAsFastPath() throws StandardException, SQLException {
System.out.println("SPARQL.executeAsFastPath");	
		if ( isCached(whereText.toString()) ) logger.logInfo("Rows cached - no need to send SPARQL REST query");
		else {
			try {
				String query = getConfigFileTextWithReplacements().replaceAll("\\[FILTER\\]", filterText.toString());
				String urlString = getVTIPropertyWithReplacements("url");
				logger.logInfo("Rows not cached - making SPARQL REST query. url: "+urlString+", query: "+ query);
				URL url = new URL(urlString + "?query=" + URLEncoder.encode(query, "UTF-8"));
				URLConnection urlc = url.openConnection();
				

				Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(urlc.getInputStream());
				results = doc.getElementsByTagName("result");

			} catch (Exception e) {
				e.printStackTrace();
//				throw new SQLException(e.getMessage()); // only ever return true from this method, false makes Derby run executeQuery() instead
			}
		}
		return true;
	}
	
	@Override public boolean reinitialise() throws Exception { return true; }

	public void setQualifiers(VTIEnvironment vtie, Qualifier[][] qual) throws SQLException {
System.out.println("SPARQL.SETQUALIFIERS");
		if (vtie != null) {
			String numRows = vtie.getOriginalSQL().replaceAll(".*(?i)FETCH FIRST (\\d+) ROWS ONLY", "$1");
			if (numRows.length() != vtie.getOriginalSQL().length()) {
				try {
					int limit = Integer.parseInt(numRows);
					replacements.set(0, limit + "");
				} catch (NumberFormatException e) {
				}
			}
		}
		if (qual != null) {
			try {
				for (int i = 0; i < qual.length; i++) {
					StringBuilder filter = new StringBuilder("FILTER (");
					for (Qualifier q : qual[i]) {
						
						//determine the column name and operator for this qualifier

						whereText.append(getMetaData().getColumnName(q.getColumnId() + 1).toLowerCase()
						+ getOperatorFor(q.getOperator()));
						
						String variableName = getMetaData().getColumnName(q.getColumnId() + 1).toLowerCase();
						//work out what type we are considering and add a suitably encoded value.
						String typeName = q.getOrderable().getTypeName();
						
						SparqlType sparqlType = sparqlTypes.get(variableName);
;
						if (sparqlType == null){
							if (typeName.toLowerCase().startsWith("varchar")){
								sparqlType = SparqlType.STR;
							} else {
								//The current prototype code only supports Strings and Numeric types.
								sparqlType = SparqlType.NUM;
							}
						}
						
						// determine the relevant SPARQL filter statement
						filter.append("?" + variableName 
								+ getOperatorFor(q.getOperator()));
						
						switch (sparqlType) {
						case IRI:
							filter.append('<'+ q.getOrderable().getString()+'>');
							break;
						case STR:
							filter.append('\"'+ q.getOrderable().getString()+'\"');
							break;
						case NUM:
							filter.append(q.getOrderable().getDouble());
							break;
						}
						
						// determine the relevant SQL where clause 
						if (typeName.toLowerCase().startsWith("varchar")){
							whereText.append( '\''+ q.getOrderable().getString()+'\'');
						} else {
							whereText.append( q.getOrderable().getDouble());
						}
						
						if (i == 0) {
							filter.append(" && ");
							whereText.append (" AND ");
						} else {
							filter.append(" || ");
							whereText.append(" OR  ");
						}
					}
					filter.delete(filter.length() - 3,filter.length());
					whereText.delete(whereText.length() - 4, whereText.length());
					filter.append(") .\r\n");
					filterText.append(filter);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public double getEstimatedCostPerInstantiation(VTIEnvironment arg0)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean supportsMultipleInstantiations(VTIEnvironment arg0)
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
}
