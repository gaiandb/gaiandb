/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;



import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.SQLBit;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLClob;
import org.apache.derby.iapi.types.SQLDate;
import org.apache.derby.iapi.types.SQLDecimal;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongVarbit;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLLongvarchar;
import org.apache.derby.iapi.types.SQLReal;
import org.apache.derby.iapi.types.SQLSmallint;
import org.apache.derby.iapi.types.SQLTime;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.derby.iapi.types.SQLTinyint;
import org.apache.derby.iapi.types.SQLVarbit;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.impl.sql.execute.GenericScanQualifier;

import com.ibm.gaiandb.DataSourcesManager.RDBProvider;
import com.ibm.gaiandb.diags.GDBMessages;


/**
 * @author DavidVyvyan
 */
public class RowsFilter {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "RowsFilter", 40 );
	
	private static final int MIN_PERCENT_ROWS_REMAINING_TO_JUSTIFY_USING_INDEX_RESULT = 30;
	
	/**
	 * Test qualifiers on the given row
	 * Returns true if the qualifiers are met, false otherwise.
	 * 
	 * @param dvdr
	 * @param qualifiers
	 * @return
	 * @throws SQLException
	 */
    public static boolean testQualifiers ( DataValueDescriptor[] dvdr, Qualifier[][] qualifiers ) throws SQLException {
    	
    	boolean areQualifiersMet = true;
	    	
        if ( null != qualifiers ) {
        	for (int i=0; i<qualifiers.length; i++) {
        		Qualifier[] qrow = qualifiers[i];
        		boolean testRow = ( 0==i ? true : false );
        		
        		for (int j=0; j<qrow.length; j++) {    			
        			Qualifier q = qrow[j];
        			boolean testCol = true;
        			int colID = q.getColumnId(); // 0-based
        			DataValueDescriptor dvdConstant = null;
        			
            		try {
            			dvdConstant = q.getOrderable();
						testCol = q.negateCompareResult() ^ dvdr[ colID ].compare( q.getOperator(), dvdConstant, q.getOrderedNulls(), q.getUnknownRV() );	 
						
					} catch (StandardException e) {
						throw new SQLException( "Unable to compare " + dvdr[colID] + " with " + dvdConstant + ": " + e );
					} catch (ArrayIndexOutOfBoundsException e1) {
						if ( 0 > colID )
							throw new SQLException( "Column " + (colID+1) +
								" cannot be referenced against this physical table, as its column name is not defined or doesn't match it.");
						
						throw new ArrayIndexOutOfBoundsException( e1.getMessage() );
					}
            		
//            		if ( q.negateCompareResult() ) testCol = !testCol;
            		
            		if ( 0 == i ) 	testRow = testRow && testCol; 
            		else 			testRow = testRow || testCol;
            		
            		if ( Logger.LOG_ALL == Logger.logLevel )
                	logger.logDetail("Qualifier [" + i +"][" + j + "] test: Column " + (colID+1) + ", value ["+ dvdr[ colID ] +"] "
                			+ getOrderingOperatorString(q) + ' ' + getFormattedValueOfOrderable(q)
                			+ " ? result = " + testCol + ", cumulative result for this qualifier row = " + testRow);
                	
                	// Optimisation: true || 'OR-expr' is always true; and false && 'AND-expr' is always false
                	if ( 0 == i ) { if ( !testRow ) break; }
                	else if ( testRow ) break;
    			}
        		
        		areQualifiersMet = areQualifiersMet && testRow;
        		if (!areQualifiersMet) break;
        	}
        }
        
    	if ( Logger.LOG_ALL == Logger.logLevel )
    		logger.logDetail("Qualifiers evaluated on row: " + Arrays.asList(dvdr) + ", result = " + areQualifiersMet);
        
        return areQualifiersMet;
    }
    
    /**
     * Test qualifiers on the row. Only test those that apply to columns that are not null or NULL.
     * Note: A 'null' column was never set, whereas a 'NULL' column is an instance of a certain DataValueDescriptor (e.g. a SQLChar, SQLInteger..)
     * that has not been given a value yet.
     * 
     * Returns null if the tested qualifiers are not met.
     * Otherwise, if prune is true, then a Qualifier[][] structure is returned with the remaining qualifiers which were not tested. If prune is
     * not set then an empty Qualifier[0][] structure is returned.
     * 
     * @param dvdr
     * @param qualifiers
     * @param prune
     * @return
     * @throws Exception
     */
    public static Qualifier[][] testAndPruneQualifiers ( DataValueDescriptor[] dvdr, Qualifier[][] qualifiers, boolean prune ) throws Exception {
    	
    	boolean areQualifiersMet = true;
    	List<Qualifier[]> prunedQualifiers = prune ? new ArrayList<Qualifier[]>() : null;
    	
        if ( null != qualifiers ) {
        	for (int i=0; i<qualifiers.length; i++) {
        		Qualifier[] qrow = qualifiers[i];
        		List<Qualifier> prunedRow = prune ? new ArrayList<Qualifier>() : null;
        		boolean testRow = ( 0==i ? true : false );
        		boolean isUnknown = false;
        		
        		for (int j=0; j<qrow.length; j++) {
        			Qualifier q = qrow[j];
        			int colID = q.getColumnId(); // 0-based

        			DataValueDescriptor orderable = q.getOrderable();
        			DataValueDescriptor dvdCell;
        			
        			if ( 0 > colID || dvdr.length <= colID ) {
        				// Column ID is out of range. This means the column value will *always* be unknown, i.e. NULL
        				// In this case, we want to go ahead with the comparison (which will use q.getOrderedNulls() and q.getUnknownRV())
        				logger.logDetail("RowsFilter.testAndPruneQualifiers(): Referenced column index " + (colID+1)
        						+ " in qualifiers[][] is outside of table shape range [1," + dvdr.length
        						+ "] - Using NULL value to test and prune with orderedNulls = " + q.getOrderedNulls()
        						+ ", unknownRV = " + q.getUnknownRV() + ", negateCompareResult = " + q.negateCompareResult());
        				dvdCell = orderable.getNewNull();
        			} else {
        				dvdCell = dvdr[colID];
        			
	        			// If qualifier column is not a constant, or if it cannot be
	        			// defined (due to it being associated to an unknown child node), then keep hold of it for later testing
	        			
	        			if ( null == dvdCell || dvdCell.isNull() ) { // && null == nodeDef ) ) {
	        				// Don't test this qualifier column as we don't know what its value is!
	        				// This one is not to be pruned...
	        				
	        				if (prune) prunedRow.add( q );
	        				isUnknown = true;
	        				continue;
	        			}
        			}
        			
//        			if ( dvdr[colID].isNull() && null != nodeDef ) {
//        				VTI childVTI = DataSourcesManager.getVTI( nodeDef );
//        				dvdr[colID]
//        			}
        			
        			boolean testCol = q.negateCompareResult() ^ dvdCell.compare(
        					q.getOperator(), q.getOrderable(), q.getOrderedNulls(), q.getUnknownRV() );
            		
            		if ( 0 == i ) 	testRow = testRow && testCol;
            		else 			testRow = testRow || testCol;
            		
            		if ( Logger.LOG_ALL == Logger.logLevel )
                	logger.logDetail("Qualifier [" + i +"][" + j + "] test: ColumnID " + (colID+1) + ", value ["+ dvdCell +"] "
                			+ getOrderingOperatorString(q) + ' ' + getFormattedValueOfOrderable(q)
                			+ " ? result = " + testCol + ", cumulative result for this qualifier row = " + testRow);
                	
                	// Optimisation: true || 'OR-expr' is always true; and false && 'AND-expr' is always false
                	if ( 0 == i ) { if ( !testRow ) break; }
                	else if ( testRow ) break;
    			}

        		// Keep this row if there's some unknowns in it, and if the
        		// tested qualifiers didn't cancel it out...
        		if ( 0 == i ) {
        			if ( true == testRow )
        				 // Always include this row, even if its empty, because it is the first row and all its columns are "ANDed" together
        				if (prune) prunedQualifiers.add( prunedRow.toArray( new Qualifier[0] ) );
        		
        		} else {
               		if ( false == testRow && isUnknown ) {
            			testRow = true; // We can't discount this row as there are some untested qualifiers.
            			if (prune) prunedQualifiers.add( prunedRow.toArray( new Qualifier[0] ) );
            		}
        		}
        		
        		areQualifiersMet = areQualifiersMet && testRow;
        		if (!areQualifiersMet) break;
        	}
        }
        
        if ( Logger.LOG_ALL == Logger.logLevel )
        logger.logDetail("Final Qualifier[][] tests outcome: " + areQualifiersMet + ", for record: " + Arrays.asList(dvdr) );
        
        if ( !areQualifiersMet ) return null;
        
        if ( prune ) {
        	if ( 1 < prunedQualifiers.size() || 0 < prunedQualifiers.get(0).length )
        		return prunedQualifiers.toArray( new Qualifier[0][] );
        }
        
        return new Qualifier[0][];
//        return prune ? (Qualifier[][]) prunedQualifiers.toArray( new Qualifier[0][] ) : new Qualifier[0][];
    }
    
    /**
     * Copy the qualifiers structure, but swap the column ids with the mapped physical tables' column ids.
     * This can allow us to apply qualifier tests to raw physical rows...
     * If mapping is null or 121, just perform a deep copy.
     * 
     * @param qualifiers containing column IDs that are 0-based
     * @param mappedColumns 0-based
     * @return deep copy with column IDs mapped
     * @throws Exception
     */
    public static Qualifier[][] getQualifiersDeepCopyWithColumnsMapped( Qualifier[][] qualifiers, int[] mappedColumns ) throws Exception {
    	
    	if ( null == qualifiers ) return null;
    	Qualifier[][] mappedQualifiers = new Qualifier[ qualifiers.length ][];
    	
//		logger.logInfo("Mapping Qualifiers: " + reconstructSQLWhereClause(qualifiers) + ", mappedCols " + GaianDBConfig.intArrayAsString(mappedColumns) );
    	for (int i=0; i<qualifiers.length; i++) {
    		Qualifier[] qrow = qualifiers[i];
    		mappedQualifiers[i] = new Qualifier[ qrow.length ];
        	for (int j=0; j<qrow.length; j++) {
        		Qualifier q = qrow[j];
        		GenericScanQualifier gsq = new GenericScanQualifier(); // Copying GenericQualifiers into GenericScanQualifiers... should be ok.
    			int colID = null == mappedColumns ? q.getColumnId() : mappedColumns[q.getColumnId()];
        		try {
					gsq.setQualifier( colID, q.getOrderable(), q.getOperator(), q.negateCompareResult(), q.getOrderedNulls(), q.getUnknownRV() );
				} catch (StandardException e) { throw new Exception("Failed deep Qualifier[][] copy/map, setting qualifier["+i+"]["+j+"] having colID " + colID + ", cause: " + e); }
				
        		mappedQualifiers[i][j] = gsq;
        	}
    	}
    	    	
    	return mappedQualifiers;
    }
    
    /**
     * Changes the types of the constant orderables in the qualifiers structure so that they
     * match the logical table types exactly (e.g. morph ints into bigints for orderables on bigint cols)
     * This allows us to apply comparisons and casts correctly later.
     * 
     * e.g.: When applying qualifiers to in-memory physical columns that are stored in their orginal types, 
     * we avoid a situation where the physical column type (e.g. a string representing a number > 2^31) cannot be cast 
     * to the orderable type (e.g. int) when it could have been cast to the logical table column type (e.g. bigint).
     * 
     * @param qualifiers
     * @param ltrsmd
     * @throws SQLException
     */    
    public static void morphQualifierOrderablesIntoLTTypes( 
    		Qualifier[][] qualifiers, GaianResultSetMetaData ltrsmd ) throws SQLException {
    	
    	if ( null == qualifiers ) return;
    	
    	for (int i=0; i<qualifiers.length; i++) {
    		Qualifier[] qrow = qualifiers[i];
        	for (int j=0; j<qrow.length; j++) {
        		Qualifier q = qrow[j];
        		// Note that we are copying GenericQualifiers into GenericScanQualifiers... should be ok.
        		GenericScanQualifier gsq = new GenericScanQualifier();
        		int colID = q.getColumnId(); // 0-based
        		DataValueDescriptor orderable = null;
        		try {
        			orderable = q.getOrderable();
        			DataValueDescriptor ltTypeDVD = constructDVDMatchingJDBCType( ltrsmd.getColumnType(colID+1) );
        			ltTypeDVD.setValue( orderable );
        			
        			gsq.setQualifier( colID, ltTypeDVD,
        					q.getOperator(), q.negateCompareResult(), q.getOrderedNulls(), q.getUnknownRV() );
				} catch (StandardException e) {
					throw new SQLException("Unable to cast orderable " + orderable + " to column type " +
							ltrsmd.getColumnTypeNameGaianDB(colID+1) + " of column '" + ltrsmd.getColumnName(colID+1) + "': " + e);
				}
				qrow[j] = gsq;
        	}
    	}
    }
    
    // Only used in AbstractVTI...
//    public static Qualifier cloneQualifierWithMappedColumnID( Qualifier q, int[] columnsMapping ) throws SQLException {
//		GenericScanQualifier gsq = new GenericScanQualifier();
//		try {
//			gsq.setQualifier( columnsMapping[ q.getColumnId() ], q.getOrderable(),
//					q.getOperator(), q.negateCompareResult(), q.getOrderedNulls(), q.getUnknownRV() );
//		} catch (StandardException e) {
//			throw new SQLException("Unable to clone Qualifier with column index " + q.getColumnId()
//					+ " to another with column index " + columnsMapping[ q.getColumnId() ] + "': " + e);
//		}
//		return gsq;
//    }
    
//    /**
//     * Sets all orderable values being compared to a certain column to a certain value
//     * 
//     * @param qualifiers
//     * @param colIndex
//     * @param value
//     * @throws SQLException
//     */
//    public static void setQualifierOrderable(
//    		Qualifier[][] qualifiers, int colIndex, DataValueDescriptor value ) throws SQLException {
//    	
//    	if ( null == qualifiers ) return;
//    	    	
//    	for (int i=0; i<qualifiers.length; i++) {
//    		Qualifier[] qrow = qualifiers[i];
//        	for (int j=0; j<qrow.length; j++) {
//        		Qualifier q = qrow[j];
//    			if ( q.getColumnId() == colIndex ) { // 0-based
//	        		// Note that we are copying GenericQualifiers into GenericScanQualifiers... should be ok.
//	        		GenericScanQualifier gsq = new GenericScanQualifier();
//	        		try {
//	        			gsq.setQualifier( colIndex, value, q.getOperator(), q.negateCompareResult(), q.getOrderedNulls(), q.getUnknownRV() );
//					} catch (Exception e) {
//						throw new SQLException("Unable to setQualifierOrderable value " + value + " for column id " + colIndex + ": " + e);
//					}
//					qrow[j] = gsq;
//    			}
//        	}
//    	}
//    }
    
//    public static HashSet getColumnIDsUsedInQualifiers( Qualifier[][] qualifiers ) {
//    	
//    	if ( null == qualifiers ) return new HashSet();
//    	
//    	HashSet colIDs = new HashSet();
//    	
//    	for (int i=0; i<qualifiers.length; i++) {
//    		Qualifier[] qrow = qualifiers[i];
//        	for (int j=0; j<qrow.length; j++) {
//        		Qualifier q = qrow[j];
//        		colIDs.add( new Integer( q.getColumnId() ) ); // 0-based
//        	}
//    	}
//    	
//    	return colIDs;
//    }
    
    /**
     * Applies qualifiers that involve indexed columns to the indexes, whilst factoring out those qualifiers
     * once applied.
     * If this operation suitably decreases the size of one of the index's TreeMap structure, then the method
     * returns the diminished structure, and the qualifiers structure will also be pruned.
     * 
     * Otherwise null is returned and the qualifiers' structure is un-changed.
     * 
     * Returns null in any of the following cases:
     * 		- qualifiers is null
     * 		- indexes contains no indexes
     * 		- qualifiers had little or no impact on the indexes
     * 
     * @param indexes
     * @param qualifiers
     * @return
     */
    public static SortedMap<DataValueDescriptor, Object> applyAndPruneIndexQualifiers( ConcurrentMap<Integer, SortedMap<DataValueDescriptor, Object>> indexes, Qualifier[][] qualifiers ) {
    	
    	if ( null == qualifiers )
    		return null;
    	
    	List<Qualifier[]> prunedQualifiers = new ArrayList<Qualifier[]>();
    	
    	int indexedCol = -1;
    	SortedMap<DataValueDescriptor, Object> index = null;
    	Iterator<Integer> iter = indexes.keySet().iterator();
    	if ( iter.hasNext() ) {
    		Integer colIDInteger = iter.next();
    		indexedCol = colIDInteger.intValue();
    		index = indexes.get( colIDInteger );
    	} else
    		return null;
    	
    	int indexsize = index.size();
    	
    	// If Qualifiers have little or no impact on the index, then return null
//    	boolean indexMapWasReducedEnough = false;
		
    	// This map is used in case we eliminate all rows or find a single one
    	SortedMap<DataValueDescriptor, Object> newMap = new TreeMap<DataValueDescriptor, Object>();
    	
    	try {
    	
    		// Just deal with the first row of anded conditions first
    		
    		logger.logInfo("Applying predicates to index, number of qualifier rows: " + qualifiers.length);
    		
    		Qualifier[] qrow = qualifiers[0];
    		List<Qualifier> prunedRow = new ArrayList<Qualifier>();
    		
			SortedMap<DataValueDescriptor, Object> submap = index;
    		
    		for (int j=0; j<qrow.length; j++) {
    			
    			Qualifier q = qrow[j];
    			
    			// Keep the qualifiers that don't act on the indexed column
    			if ( q.getColumnId() != indexedCol ) {
    				prunedRow.add( q );
    				continue;
    			}
    			    			
    			int operator = q.getOperator();
    			DataValueDescriptor dvd = q.getOrderable();
    			boolean negate = q.negateCompareResult();
    			
    			logger.logInfo("Deriving submap from predicate on index col: " + (indexedCol+1) + ", comparing key with: " + dvd + ", negate = " + negate);
    			
    			if ( negate ) switch ( operator ) {
					case Orderable.ORDER_OP_EQUALS: prunedRow.add( q ); continue; // A != operator has no impact on an index
    				case Orderable.ORDER_OP_LESSTHAN: operator = Orderable.ORDER_OP_GREATEROREQUALS; break;
    				case Orderable.ORDER_OP_GREATEROREQUALS: operator = Orderable.ORDER_OP_LESSTHAN; break;
    				case Orderable.ORDER_OP_GREATERTHAN: operator = Orderable.ORDER_OP_LESSOREQUALS; break;
    				case Orderable.ORDER_OP_LESSOREQUALS: operator = Orderable.ORDER_OP_GREATERTHAN; break;
    			}

    			if ( Orderable.ORDER_OP_LESSTHAN != operator && Orderable.ORDER_OP_GREATEROREQUALS != operator ) {
    			
    				Object row = submap.get( dvd );
    				
    				if ( Orderable.ORDER_OP_EQUALS == operator ) {
    					logger.logInfo( "OP_EQUALS: Found rows with matching key in index" );
    					newMap.put( dvd, row );
    					// could also update qualifiers here but there's little point
    					return newMap;
    				}
    				
					logger.logInfo( "Switching OP_GREATERTHAN or OP_LESSOREQUALS operator to apply headMap or tailMap function" );
    				
    				// The operator must be Orderable.ORDER_OP_LESSOREQUALS or Orderable.ORDER_OP_GREATERTHAN
        				
					// We need to find the next value down if the one we have is in the map, because
					// methods headMap() and tailMap() (used later) respectively exclude and include the compared value...
					if ( null != row ) {
						
    					logger.logInfo( "Matched key value in index - finding next key value..." );
    					
						SortedMap<DataValueDescriptor, Object> tailMap = submap.tailMap( dvd );
						// We need to shift down to the next row so it is either included or excluded (depending on operator)
						Iterator<DataValueDescriptor> keysIterator = tailMap.keySet().iterator(); // keySet() off a TreeMap returns ordered keys
						keysIterator.next(); // skip the row we know about
						
						if ( ! keysIterator.hasNext() ) {

	    					switch ( operator ) {
    		    				case Orderable.ORDER_OP_GREATERTHAN:
    		    					logger.logInfo( "OP_GREATERTHAN: No rows satisfy this condition, returning empty map" );
    		    					return newMap;
    		    				case Orderable.ORDER_OP_LESSOREQUALS:
    		    					logger.logInfo( "OP_LESSOREQUALS: All rows satisfy this condition, no map reduction" );
    		    					continue;
	    					}
						}

						dvd = (DataValueDescriptor) keysIterator.next();
	    				logger.logInfo( "Found next comparison key: " + dvd );
					}
					
					// We can now exclude or include the row with the following operators
					switch ( operator ) {
	    				case Orderable.ORDER_OP_GREATERTHAN: operator = Orderable.ORDER_OP_GREATEROREQUALS; break;
	    				case Orderable.ORDER_OP_LESSOREQUALS: operator = Orderable.ORDER_OP_LESSTHAN; break;
					}
    			}
    			
    			boolean comparatorLessThanRange = 0 > dvd.compare( (DataValueDescriptor) submap.firstKey() );
    			boolean comparatorGreaterThanRange = 0 < dvd.compare( (DataValueDescriptor) submap.lastKey() );
    			
    			logger.logInfo("Checked ranges: " + 
    					dvd + "<" + submap.firstKey() + " " + comparatorLessThanRange + ", " + 
    					dvd + ">" + submap.lastKey() + " " + comparatorGreaterThanRange);
    			
    			if ( Orderable.ORDER_OP_LESSTHAN == operator ) {
    				
    				if ( comparatorLessThanRange ) {
    					logger.logInfo( "OP_LESSTHAN: No rows satisfy this condition, returning empty map" );
    					return newMap;
    				}
    				if ( comparatorGreaterThanRange ) {
    					logger.logInfo( "OP_LESSTHAN: All rows satisfy this condition, no map reduction" );
    					continue;
    				}
    				
    				logger.logInfo( "Getting headMap to (excl) " + dvd );
    				submap = submap.headMap( dvd );
    				
    			} else { // Orderable.ORDER_OP_GREATEROREQUALS == operator
    				
    				if ( comparatorGreaterThanRange ) {
    					logger.logInfo( "OP_GREATEROREQUALS: No rows satisfy this condition, returning empty map" );
    					return newMap;
    				}
    				if ( comparatorLessThanRange ) {
    					logger.logInfo( "OP_GREATEROREQUALS: All rows satisfy this condition, no map reduction" );
    					continue;
    				}

    				logger.logInfo( "Getting tailMap from (incl) " + dvd );
    				submap = submap.tailMap( dvd );
    			}
    			
    			if ( submap.isEmpty() ) //1 > submap.size() )
    				return newMap;
    			
    			logger.logInfo("Reduced submap range to: " + submap.firstKey() + " -> " + submap.lastKey() );
    		}
    		
    		prunedQualifiers.add( prunedRow.toArray( new Qualifier[0] ) );
    		
    		// Note submap.size() can be time consuming as it will iterate through the submap.
    		// index.size() however is instantaneous as the value is cached.
//    		if ( submap.size() < index.size() / 2 )
//    			indexMapWasReducedEnough = true;
    		
    		if ( isSubMapReducedEnough( submap, indexsize ) ) {
    			qualifiers[0] = prunedQualifiers.get(0);
    			logger.logInfo("Returning reduced submap of index, remaining predicates: " + reconstructSQLWhereClause(qualifiers));
    			return submap;
    		} 
    		else {
    			logger.logInfo("Index submap was not reduced enough, using basic rows structure to do a table scan on all predicates");
    			return null;
    		}
    		
    		
	    	
//    		// Each full susequent qualifier row deals with an or'ed condition.
//    		// If an or'ed condition deals only with conditions on an index, then we can reduce the submap with 
//    		// the full resulting condition.
//    		// Each or'ed condition is applied such that it increases the range previously obtained
//    		SortedMap oredConditionMap = submap;
//    		
//	    	for (int i=1; i<qualifiers.length; i++) {
//	    		
//	    		// Now deal with each subsequent row of or'ed conditions
//	    		
//	    		qrow = qualifiers[i];
//	    		prunedRow = new ArrayList();
//
//    			boolean allColsAreOnIndex = true;
//	    		
//	    		for (int j=0; j<qrow.length; j++) {
//	    			Qualifier q = qrow[j];
//	    			
//	    			// If not all qualifiers of an or'ed row deal with the indexed column, then put the whole row aside for 
//	    			// table scanning later as we can't take advantage of an index in this situation
//	    			if ( q.getColumnId() != indexedCol ) {
//
//		    			allColsAreOnIndex = false;
//	    				prunedRow.add( q );
//	    				break;
//	    			}
//	    			
//	    			int operator = q.getOperator();
//	    			DataValueDescriptor dvd = q.getOrderable();
//	    			boolean negate = q.negateCompareResult();
//	    			
////	    			what to do now ???? - we wd need to grow the index somhow - but this may lead to multiple ranges... so multiple maps...
//	    			
//	    		}
//	    		
//	    		if ( 0 == i || !prunedRow.isEmpty() )
//	    			prunedQualifiers.add( prunedRow.toArray( new Qualifier[0] ) );
//	    	}
    	
    	} catch ( Exception e ) {
    		logger.logThreadException(GDBMessages.ENGINE_APPLY_QUALIFIERS_ERROR, "Unable to apply qualifiers on index (ignoring index)", e);
    		return null;
    	}
    	
//        return null;
    }
    
    public static int[] getAllSortedColsInvolvedInQuery( int[] projection, Qualifier[][] qualifiers ) {
    	
    	// hs must be ordered
    	TreeSet<Integer> hs = new TreeSet<Integer>();
    	// Get integers from projected cols - note these are 1-based, so we substract 1
    	if ( null != projection )
    	for (int i=0; i<projection.length; i++) hs.add( new Integer(projection[i]-1) );
    	
    	// Now get the col ids involved in the qualifiers' structure
        if ( null != qualifiers ) {
        	for (int i=0; i<qualifiers.length; i++) {
        		Qualifier[] qrow = qualifiers[i];
        		
        		for (int j=0; j<qrow.length; j++)
        			hs.add( new Integer( qrow[j].getColumnId() ) ); // 0-based
        	}
        }
        
        int[] res = new int[ hs.size() ];
        Iterator<Integer> it = hs.iterator();
        int i = 0;
        while( it.hasNext() )
        	res[i++] = ((Integer) it.next()).intValue();
        
        return res;
    }
    
    private static boolean isSubMapReducedEnough( SortedMap<DataValueDescriptor, Object> submap, int indexSize ) {
    	
    	// Note this method itself is expensive when the index fails to reduce the number of rows
    	// significantly.
    	
    	int size = 0;
    	int threshold = MIN_PERCENT_ROWS_REMAINING_TO_JUSTIFY_USING_INDEX_RESULT * ( indexSize / 100 );
    	
    	// Determine worth of keeping submap, as extracting N rows from it is more costly than table scan on N rows.
		logger.logInfo("Checking remaining submap size: must be < " + threshold);
		
        Iterator<DataValueDescriptor> i = submap.keySet().iterator();
        while (i.hasNext()) {
            if ( threshold < size++ ) return false;
            i.next();
        }
        
        return true;
    }
        
    public static Qualifier[][] factorOutColumnPredicates( Qualifier[][] qualifiers, int arrayIndexOfColumn ) {
    	
    	List<Qualifier[]> prunedQualifiers = new ArrayList<Qualifier[]>();
    	
        if ( null != qualifiers ) {
        	for (int i=0; i<qualifiers.length; i++) {
        		Qualifier[] qrow = qualifiers[i];
        		List<Qualifier> prunedRow = new ArrayList<Qualifier>();
        		
        		for (int j=0; j<qrow.length; j++) {
        			Qualifier q = qrow[j];
        			
        			// Only keep the qualifiers that don't act on the targeted column
        			if ( q.getColumnId() != arrayIndexOfColumn )
        				prunedRow.add( q );
        		}
        		
        		if ( 0 == i || !prunedRow.isEmpty() )
        			prunedQualifiers.add( prunedRow.toArray( new Qualifier[0] ) );
        	}
        }
        
        if ( null == qualifiers ||
        		( 1 == prunedQualifiers.size() && 0 == ((Qualifier[]) prunedQualifiers.get(0)).length ) )
        	return null;        	
        
        return (Qualifier[][]) prunedQualifiers.toArray( new Qualifier[0][] );
    }
    
    public static Qualifier[][] factorOutColumnPredicatesCollectingOrderables( 
    		Qualifier[][] qualifiers, int[] targettedColumnIndices, DataValueDescriptor[] orderableValuesOut ) throws SQLException {
    	
    	List<Qualifier[]> prunedQualifiers = new ArrayList<Qualifier[]>();
    	
        if ( null != qualifiers ) {
        	for (int i=0; i<qualifiers.length; i++) {
        		Qualifier[] qrow = qualifiers[i];
        		List<Qualifier> prunedRow = new ArrayList<Qualifier>();
        		
        		for (int j=0; j<qrow.length; j++) {
        			Qualifier q = qrow[j];
        			int targettedIndex = -1;
        			
//        			logger.logInfo("Scanning next qualifier column index: " + q.getColumnId());
        			
        			// Only keep the qualifiers that don't act on the targetted columns (which are relative to 1)
        			if ( -1 == ( targettedIndex = Util.intArrayContains( targettedColumnIndices, q.getColumnId()+1 ) ) )
        				prunedRow.add( q );
        			else try {
        				orderableValuesOut[ targettedIndex ] = q.getOrderable();
        			} catch ( StandardException e ) {
        				String errmsg = "Could not get Orderable value for special column (e.g. query id or propagation count) from Qualifiers: " + e;
        				logger.logThreadWarning(GDBMessages.ENGINE_QUALIFIERS_SPECIAL_COLUMN_ERROR, "DERBY ERROR: " + errmsg);
        				throw new SQLException( errmsg );
        			}
        		}
        		
        		if ( 0 == i || !prunedRow.isEmpty() )
        			prunedQualifiers.add( prunedRow.toArray( new Qualifier[0] ) );
        	}
        }
        
        if ( null == qualifiers ||
        		( 1 == prunedQualifiers.size() && 0 == ((Qualifier[]) prunedQualifiers.get(0)).length ) )
        	return null;        	
        
        return (Qualifier[][]) prunedQualifiers.toArray( new Qualifier[0][] );
    }
    
	public static String reconstructSQLWhereClause( Qualifier[][] sqlQualifiers ) {
		return reconstructSQLWhereClause( sqlQualifiers, null, null, RDBProvider.Derby, null );
	}
	
	public static String reconstructSQLWhereClause( Qualifier[][] sqlQualifiers, GaianResultSetMetaData logicalTableRSMD ) {
		return reconstructSQLWhereClause( sqlQualifiers, logicalTableRSMD, null, RDBProvider.Derby, null );
	}
    
	/**
	 * Parses the sqlQualifiers predicates structure and returns a SQL string "where-clause" representation of them.
	 * 
	 * The next 2 arguments are used to find column names to populate the string with. When only the logical table result set meta data
	 * is provided, the logical column names will be used. When not even the meta data is provided, columns are simply represented
	 * by logical table index position, i.e. as C1, C2, C3, ...
	 * 
	 * The physicalColTypes argument is used to compare logical and physical column types, and determine if an explicit CAST is required. 
	 * 
	 * @param sqlQualifiers - The predicates structure to be parsed - contains CNF representation of comparison predicates.
	 * @param logicalTableRSMD - The logical table's result set meta data - used to find column names which are not physical.
	 * @param ltPhysicalColNames - The physical column names of the logical table column ids. The logical table also defines
	 * constant columns or NULL columns which lie beyond the range of the physical columns. 
	 * @param physicalColTypes - Used to determine if an explicit CAST is required when applying the predicates to physical columns.
	 * @return
	 */		
	public static String reconstructSQLWhereClause( Qualifier[][] sqlQualifiers, GaianResultSetMetaData logicalTableRSMD, 
			String[] ltPhysicalColNames, RDBProvider rdbmsProvider, int[] physicalColTypes) { //, boolean insertWherePrefixForExistingClause ) {
		
		StringBuffer sqlWhereClause = new StringBuffer("");
		try {
//			logger.logDetail("sqlQualifiers is null ? " + (null == sqlQualifiers ? "true" : "false, len " + sqlQualifiers.length));
			if ( null != sqlQualifiers ) {
				
//				int physicalColCount = logicalTableRSMD.getColumnCount() - GaianDBConfig.HIDDEN_COLS.length;
				
				for (int x=0; x<sqlQualifiers.length; x++) {
					
					// Note Derby passes qualifiers in conjunctive normal form:
					// e.g: "a and b and c and (d or e) and (g or h) and (j or k or l)"...
					
//					// Start with a "WHERE " if this is the first qualifier row and if it is not empty or there are other rows.
//					if ( 0 == x && ( 1 < sqlQualifiers.length || 0 < sqlQualifiers[x].length ) )
//						sqlWhereClause.append("WHERE ");
//					else
						
					if ( 0 < x ) {
						if ( 1 == x && 0 == sqlQualifiers[0].length ) {
							// No need for an 'AND'... and we only need an opening bracket if there are further qualifier[][] rows
							// e.g: "(a or b) and (c or d)..." rather than just: "a or b"
							if ( 1 < sqlQualifiers[1].length && 2 < sqlQualifiers.length ) sqlWhereClause.append('(');
						
						} else {
							if ( 1 < sqlQualifiers[x].length ) {
								// e.g: "a and b and (c or d)"
								sqlWhereClause.append(" AND (");
							} else {
								sqlWhereClause.append(" AND ");
							}
						}
					}
					
					for (int y=0; y<sqlQualifiers[x].length; y++) {
						
						if ( 0 < y ) {
							if ( 0 == x ) sqlWhereClause.append(" AND ");
							else sqlWhereClause.append(" OR ");
						}
						
						Qualifier q = sqlQualifiers[x][y];
						int colID = q.getColumnId(); // 0-based
						String colXpr = ( null != ltPhysicalColNames && colID < ltPhysicalColNames.length ) ? ltPhysicalColNames[ colID ] :
										( null != logicalTableRSMD ? logicalTableRSMD.getColumnName( colID+1 ) : "C" + (colID+1) );
						
						colXpr = GaianResultSetMetaData.wrapColumnNameForQueryingIfNotAnOrdinaryIdentifier(colXpr, rdbmsProvider);
						if (null != physicalColTypes && colID < physicalColTypes.length)
							colXpr = RowsFilter.applyProviderNormalisationTransformIfNecessary(colXpr, physicalColTypes[colID], rdbmsProvider);
						
//						if ( null != physicalColTypes )
//							logger.logInfo("Checking col types in case casting is required, lt col type: " + 
//									logicalTableRSMD.getColumnType(colID+1) + ", pt col type: " + physicalColTypes[colID] );
													
						sqlWhereClause.append( null == physicalColTypes ? colXpr :
							RowsFilter.formulateCastExpressionForPhysicalToLogicalColumnCast( colXpr, colID,
									logicalTableRSMD, physicalColTypes, rdbmsProvider ) );
						
//						sqlWhereClause.append( RowsFilter.getOrderingOperatorString( q ) );
//						sqlWhereClause.append( RowsFilter.getFormattedValueOfOrderable( q ) );
						
						String orderable = RowsFilter.getFormattedValueOfOrderable( q, rdbmsProvider );
						
						if ( null == orderable )
							sqlWhereClause.append ( q.negateCompareResult() ? " IS NOT NULL" : " IS NULL" ); // ignore operator in this case (...?)
						else {
							sqlWhereClause.append( RowsFilter.getOrderingOperatorString( q ) );
							sqlWhereClause.append( orderable );
						}
					}
					
					// Consider adding a closing bracket if this is the second row or above and it had more than one element 
					if ( 0 < x && 1 < sqlQualifiers[x].length ) {
						// Add a closing bracket as long as:
						//		   - this is the 3rd row or above
						//		or - the 1st row (of ANDs) had some elements
						//		or - there are more than 2 rows
						if ( 1 < x || 0 < sqlQualifiers[0].length || 2 < sqlQualifiers.length ) sqlWhereClause.append(')');
					}
				}
			}
		} catch ( Exception e ) {
			logger.logThreadException(GDBMessages.ENGINE_WHERE_CLAUSE_ERROR, "Exception building WHERE clause", e);
		}
		
//		if ( insertWherePrefixForExistingClause && 0 < sqlWhereClause.length() )
//			sqlWhereClause.insert(0, "WHERE ");
		
//		printThreadInfo( nodeDefNames[nodeIndex], "Set 'WHERE' clause to: " + sqlWhereClause);
		
		return sqlWhereClause.toString();
	}
    
	private static String getOrderingOperatorString( Qualifier q ) throws SQLException {
		int operator = q.getOperator();
		boolean negate = q.negateCompareResult();
		switch ( operator ) {
			case Orderable.ORDER_OP_EQUALS: return negate ? "!=" : "=";
			case Orderable.ORDER_OP_GREATEROREQUALS: return negate ? "<" : ">=";
			case Orderable.ORDER_OP_GREATERTHAN: return negate ? "<=" : ">";
			case Orderable.ORDER_OP_LESSOREQUALS: return negate ? ">" : "<=";
			case Orderable.ORDER_OP_LESSTHAN: return negate ? ">=" : "<";
		}
		String errmsg = "Invalid operator detected (not one of the Orderable interface): " + operator;
		logger.logThreadWarning(GDBMessages.ENGINE_OPERATOR_INVALID, "DERBY ERROR: " + errmsg);
		throw new SQLException( errmsg );
	}
	
	private static String getFormattedValueOfOrderable( Qualifier q ) throws SQLException {
		return getFormattedValueOfOrderable( q, RDBProvider.Derby );
	}
	
	private static String getFormattedValueOfOrderable( Qualifier q, RDBProvider rdbmsProvider ) throws SQLException {

		try {
			DataValueDescriptor dvd = q.getOrderable();
			if ( dvd.isNull() ) return null;
			String value = dvd.getString();
			int jdbcType = TypeId.getBuiltInTypeId( dvd.getTypeName() ).getJDBCTypeId();
//			logInfo("Getting value for JDBC type: " + jdbcType);
			
			// TODO: Coallesce empty strings to nulls if provider stores nulls as "".
			
			switch ( jdbcType ) {
				case Types.CHAR: case Types.VARCHAR: case Types.LONGVARCHAR: case Types.CLOB: return "'" + value + "'";
				case Types.DATE: case Types.TIME: case Types.TIMESTAMP:
					return ( rdbmsProvider.equals( RDBProvider.Oracle ) ? "TIMESTAMP " : "" ) + "'" + value + "'";
				default: return value;
			}
			
		} catch (StandardException e) {
			String errmsg = "Could not get Orderable value from Qualifier: " + e;
			logger.logThreadWarning(GDBMessages.ENGINE_QUALIFIER_ORDERABLE_ERROR, "DERBY ERROR: " + errmsg);
			throw new SQLException( errmsg );
		}
	}
	
	private static String formulateCastExpressionForPhysicalToLogicalColumnCast( String colName, int colID,
			GaianResultSetMetaData ltrsmd, int[] pColTypes, RDBProvider rdbms ) {
		
		// If we get to here it must mean we are dealing with a physical data source, and that the column
		// exists in it. If the logical column type does not match the physical one, then we need
		// to cast the physical one to it in the reconstructed SQL.
		
		int pColTypeNormalised = normaliseToGaianColType( pColTypes[colID], rdbms );
		
//		logger.logDetail("Checking to apply CAST if back end type differs for: " + colName
//				+ ", lColType: " + ltrsmd.getColumnType(colID+1) + ", pColType: " + pColType);
		
		if ( ltrsmd.getColumnType(colID+1) == pColTypeNormalised ) return colName;
		
		String ltTypeName = ltrsmd.getColumnTypeDescriptionDerby(colID+1); //TypeId.getBuiltInTypeId( logicalTypeID ).getSQLTypeName();
		return "cast(" + colName + " as " + ltTypeName + ")";
	}
	
	private static int normaliseToGaianColType( int pColType, RDBProvider rdbmsProvider ) {
		
		switch ( rdbmsProvider ) {
			case DB2:
				switch ( pColType ) {
					case Types.OTHER:	return Types.DECIMAL; // DB2 uses Types.OTHER for DECFLOAT
				}
				break;
			case Oracle:
				// See: http://ss64.com/ora/syntax-datatypes.html
				// See: http://docs.oracle.com/cd/E11882_01/appdev.112/e13995/constant-values.html
				switch ( pColType ) {
//					case Types.NUMERIC: if ( scale was negative ) return Types.FLOAT;
			    	case 101:			return Types.DOUBLE;// BINARY_DOUBLE
				    case 100:			return Types.FLOAT;	// BINARY_FLOAT
					case -100:					// TIMESTAMPNS - deprecated to TIMESTAMP from V9.2.0
					case -102:  				// TIMESTAMP WITH LOCAL TIME ZONE
					case -101:  		return Types.TIMESTAMP; // TIMESTAMP WITH TIME ZONE - normalised using: <TSTZ_COL> AT LOCAL
					case -103:  		return Types.INTEGER;	// INTERVAL YEAR TO MONTH - normalised to number of months
					case -104:			return Types.DOUBLE;	// INTERVAL DAY TO SECOND - normalised to seconds with floating point value
				    case Types.ROWID:
				    case Types.OTHER:	return Types.VARCHAR; // Oracle uses Types.OTHER for UROWID
				}
				break;
			case MSSQLServer:
				switch ( pColType ) {
					case -16:			return Types.VARCHAR; // XML
				}
				break;
			case Derby: case MySQL: case Other: default: 
				break;
		}
		
		// Conditions for common types for all providers
		switch ( pColType ) {
			case Types.LONGVARCHAR:	return Types.VARCHAR; // all back-end LONGVARCHARS are imported as VARCHAR(32672) in Gaian.
		}
		
		return pColType;
	}
	
	/**
	 * Apply transformation to column queried in a back end RDBMS - in cases where we would otherwise obtain incoherent values.
	 * Example: Oracle type: TIMESTAMP WITH TIMEZONE. Oracle stores a timestamp value and time zone value for this type, but only
	 * returns the timestamp value when it is queried. However, predicates against the column take the timezone modifier into account.
	 * Derby has no notion of time zones with timestamps, therefore we need to normalise values extracted from such columns.
	 * 
	 * @param pColName
	 * @param pColType
	 * @param rdbmsProvider
	 * @return
	 */
	final static String applyProviderNormalisationTransformIfNecessary(
			final String pColName, final int pColType, final RDBProvider rdbmsProvider ) {
		
		// Future TODO extension: Make Normalisation function configurable from gaiandb_config.properties somehow..
		
		if ( rdbmsProvider.equals(RDBProvider.Oracle) )
			switch (pColType) {
				case -101: // TIMESTAMP WITH TIME ZONE
					return pColName+" AT LOCAL";
				case -103: // INTERVAL YEAR TO MONTH (example to create one: NUMTOYMINTERVAL(13, 'month')
					return "(EXTRACT(MONTH FROM "+pColName+")"
					     + "+EXTRACT(YEAR FROM "+pColName+")*12)";
				case -104: // INTERVAL DAY TO SECOND (example to create one: NUMTODSINTERVAL(58.3, 'hour'))
					return "(EXTRACT(SECOND FROM "+pColName+")"
					     + "+EXTRACT(MINUTE FROM "+pColName+")*60"
					     + "+EXTRACT(HOUR FROM "+pColName+")*3600"
					     + "+EXTRACT(DAY FROM "+pColName+")*86400)";
			}
		
		return pColName;
	}

	public static DataValueDescriptor constructDVDMatchingJDBCType( int jdbcType ) {
		return constructDVDMatchingJDBCType(jdbcType, RDBProvider.Derby);
	}
	
	public static DataValueDescriptor constructDVDMatchingJDBCType( int jdbcType, RDBProvider provider ) {
		
		logger.logDetail("constructDVDMatchingJDBCType(): jdbcType " + jdbcType + " RDBMS Provider " + provider);
		// Build an appropriate DataValueDescriptor for this column
		switch ( jdbcType ) {
		
			// Note BIT is a single binary bit, and the recommended Java mapping for the JDBC BIT type is as a Java boolean:
			// http://java.sun.com/j2se/1.3/docs/guide/jdbc/getstart/mapping.html
		
			// If we were to map BIT to SQLBit(), then we couldn't set its value to an int.
			// However we *can* set the value of a SQLBoolean() to a byte[].
		
		    case Types.DECIMAL: case Types.NUMERIC: return new SQLDecimal();
		    case Types.CHAR: return new SQLChar();
		    case Types.VARCHAR: return new SQLVarchar();
		    case Types.LONGVARCHAR: return new SQLLongvarchar();
		    case Types.VARBINARY: return new SQLVarbit();
		    case Types.LONGVARBINARY: return new SQLLongVarbit();
		    case Types.BINARY: return new SQLBit();
		    case Types.BOOLEAN: case Types.BIT: return new SQLBoolean();
		    case Types.BLOB: return new SQLBlob(); // size must be <= int bytes to be put in memory
		    case Types.CLOB: return new SQLClob(); // size must be <= int bytes to be put in memory
		    case Types.DATE: return new SQLDate();
		    case Types.TIME: return new SQLTime();
		    case Types.TIMESTAMP: return new SQLTimestamp();
		    case Types.INTEGER: return new SQLInteger();
		    case Types.BIGINT: return new SQLLongint();
		    case Types.SMALLINT: return new SQLSmallint();
		    case Types.DOUBLE: case Types.FLOAT: return new SQLDouble();
		    case Types.REAL: return new SQLReal();
			case Types.NCLOB: return new SQLClob();
			case Types.NVARCHAR: return new SQLVarchar();
			case Types.NCHAR: return new SQLChar();
			case Types.TINYINT: return new SQLTinyint();
		    
//			case Types.ARRAY: return new SQL (Array) data );
//			case Types.JAVA_OBJECT: case Types.STRUCT: return new SQL data );
//			case Types.REF: return new SQLRef( new RowHeapLocation(s)... );
//			case Types.DATALINK: return new SQL data );
//			case Types.DISTINCT: case Types.NULL: case Types.OTHER: return new SQLN Types.NULL ); // No distinct type supported
		    default: 
				switch ( provider ) {
				case DB2:
					switch ( jdbcType ) {
				    	case Types.OTHER:	return new SQLDecimal(); // DB2 uses Types.OTHER for DECFLOAT
					}
					break;
				case Oracle:
					// See: http://ss64.com/ora/syntax-datatypes.html
					// See: http://docs.oracle.com/cd/E11882_01/appdev.112/e13995/constant-values.html
					switch ( jdbcType ) {
				    	case 101:			return new SQLDouble();	// BINARY_DOUBLE
					    case 100:			return new SQLDouble();	// BINARY_FLOAT
						case -100:					// TIMESTAMPNS - deprecated to TIMESTAMP from V9.2.0
						case -102:  				// TIMESTAMP WITH LOCAL TIME ZONE
						case -101:  		return new SQLTimestamp(); // TIMESTAMP WITH TIME ZONE - normalised using: <TSTZ_COL> AT LOCAL
						case -103:  		return new SQLInteger(); // INTERVAL YEAR TO MONTH - normalised to number of months
						case -104:			return new SQLDouble();  // INTERVAL DAY TO SECOND - normalised to seconds with floating point value
					    case Types.ROWID:
					    case Types.OTHER:	return new SQLVarchar(); // Oracle uses Types.OTHER for UROWID
					}
					break;
				case MSSQLServer:
					switch ( jdbcType ) {
						case -16:			return new SQLVarchar(); // XML
					}
					break;
				case MySQL:
				case Derby:
				case Other:
				default:
					break;
				}
		}

		logger.logWarning( GDBMessages.ENGINE_DVD_BUILD_JDBC_TYPE_UNSUPPORTED, "Cannot build DVD: Unsupported JDBC type: "
				+ jdbcType + " for RDBMS Provider " + provider );
    	return null;
	}
	
	public static Object convertToType( Object o, int jdbcType ) throws Exception {
		
		DataValueDescriptor dvd = constructDVDMatchingJDBCType( jdbcType );
		
		if ( null == dvd ) throw new Exception("Unable to create Derby type for JDBC type: " + jdbcType);
		
		     if ( o instanceof BigDecimal )	dvd.setValue( ((BigDecimal) o).doubleValue() ); // covers dec and num
		else if ( o instanceof String )		dvd.setValue( (String) o );  // covers 3 char types
		else if ( o instanceof byte[] )		dvd.setValue( (byte[]) o ); // covers 3 byte types
		else if ( o instanceof Boolean )	dvd.setValue( ((Boolean) o).booleanValue() );
		else if ( o instanceof Blob )		dvd.setValue( ((Blob) o).getBytes(1, (int)((Blob)o).length()) );
		else if ( o instanceof Clob )		dvd.setValue( ((Clob) o).getSubString(1, (int)((Clob)o).length()) );
		else if ( o instanceof Date )		dvd.setValue( (Date) o );
		else if ( o instanceof Time )		dvd.setValue( (Time) o );
		else if ( o instanceof Timestamp )	dvd.setValue( (Timestamp) o );
		else if ( o instanceof Integer )	dvd.setValue( ((Integer) o).intValue() );
		else if ( o instanceof Long )		dvd.setValue( ((Long) o).longValue() );
		else if ( o instanceof Short )		dvd.setValue( ((Short) o).shortValue() );
		else if ( o instanceof Double )		dvd.setValue( ((Double) o).doubleValue() );
		else if ( o instanceof Float )		dvd.setValue( ((Float) o).floatValue() ); // covers real
		     
		return dvd.getObject();
	}
	
	// Following 2 methods are used for reading a raw row of data (stored in primitive data type form)
	// into an array of DataValueDescriptor wrappers, so as to apply predicates and return thr row in Derby's format.
	
//	public static DataValueDescriptor[] getDVDRowTemplateForRawData( Object row, int jdbcType ) {
//		
//		DataValueDescriptor dvd;
//		int numCols;
//		
//		// Build an appropriate DataValueDescriptor[] for this row
//		switch ( jdbcType ) {
//		
//		    case Types.DECIMAL: case Types.NUMERIC: dvd = new SQLDecimal(); numCols = ((int[]) row).length; break;
//		    case Types.CHAR: dvd = new SQLChar(); numCols = ((int[]) row).length; break;
//		    case Types.VARCHAR: dvd = new SQLVarchar(); numCols = ((int[]) row).length; break;
//		    case Types.LONGVARCHAR: dvd = new SQLLongvarchar(); numCols = ((int[]) row).length; break;
//		    case Types.VARBINARY: dvd = new SQLVarbit(); numCols = ((int[]) row).length; break;
//		    case Types.LONGVARBINARY: dvd = new SQLLongVarbit(); numCols = ((int[]) row).length; break;
//		    case Types.BINARY: dvd = new SQLBit(); numCols = ((int[]) row).length; break;
//		    case Types.BOOLEAN: case Types.BIT: dvd = new SQLBoolean(); numCols = ((int[]) row).length; break;
//		    case Types.BLOB: dvd = new SQLBlob(); numCols = ((int[]) row).length; break;
//		    case Types.CLOB: dvd = new SQLClob(); numCols = ((int[]) row).length; break;
//		    case Types.DATE: dvd = new SQLDate(); numCols = ((int[]) row).length; break;
//		    case Types.TIME: dvd = new SQLTime(); numCols = ((int[]) row).length; break;
//		    case Types.TIMESTAMP: dvd = new SQLTimestamp(); numCols = ((int[]) row).length; break;
//		    case Types.INTEGER: dvd = new SQLInteger(); numCols = ((int[]) row).length; break;
//		    case Types.BIGINT: dvd = new SQLLongint(); numCols = ((int[]) row).length; break;
//		    case Types.SMALLINT: dvd = new SQLSmallint(); numCols = ((int[]) row).length; break;
//		    case Types.TINYINT: dvd = new SQLTinyint(); numCols = ((int[]) row).length; break;
//		    case Types.DOUBLE: case Types.FLOAT: dvd = new SQLDouble(); numCols = ((int[]) row).length; break;
//		    case Types.REAL: dvd = new SQLReal(); numCols = ((int[]) row).length; break;
////			case Types.ARRAY: dvd = new SQL (Array) data ); numCols = ((int[]) row).length; break;
////			case Types.JAVA_OBJECT: case Types.STRUCT: dvd = new SQL data ); numCols = ((int[]) row).length; break;
////			case Types.REF: dvd = new SQLRef( new RowHeapLocation(s)... ); numCols = ((int[]) row).length; break;
////			case Types.DATALINK: dvd = new SQL data ); numCols = ((int[]) row).length; break;
////			case Types.DISTINCT: case Types.NULL: case Types.OTHER: dvd = new SQLN Types.NULL ); numCols = ((int[]) row).length; break; // No distinct type supported
//		    default: logger.logWarning( "Cannot build DVD row: Unsupported JDBC type: " + jdbcType ); return null;
//		}
//		
//		DataValueDescriptor[] dvdr = null;
//		for (int i=0; i<numCols; i++) dvdr[i] = dvd.getNewNull();
//		
//		return dvdr;
//	}
//	
//	public static void populateDVDRowFromRawData( Object row, DataValueDescriptor[] dvdr, int jdbcType ) {
//		
//		int typeFormatID = dvdr[0].getTypeFormatId();
//		
//		switch ( dvdr[0].getTypeFormatId() ) {
//			
//			case Stored
//		
//		}
//	}
}
