/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;

public class EntityMatrixJoiner {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "EntityMatrixJoiner", 30 );
	
	private final int MAX_LINKS; // = 5;
	
	private int[][] matrix;
	private int[] linkCounts;
	
	private int[] remainingGroups;
	private int[] restrictedSizeGroupsRemaining;
	
	private int maxCascadedOverflowDepth = 0;
	private int cascadedOverflowDepth = 0;
	
	private int numGroups = 0;
	
	public static void main(String[] args) throws IOException {
	
		Logger.setPrintStream( System.out );
		Logger.setLogLevel( Logger.LOG_MORE );
		
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmat_tiny2.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmat_tim2.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmat2.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmatbig_mod.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmatbig2.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmatmed1tail.txt";
		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmatmed.txt"; // a tenth of the file
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmatmed2.txt"; // a fifth of the file
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmatmed5.txt"; // half of the file
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\rmatbig_mod.txt";// the full file
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\tim_100.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\GDB52-emj.txt";
		
		EntityMatrixJoiner emj = new EntityMatrixJoiner( filename, 100000000, 5 );
		emj.processJoins( 6 );
	}
	
	public void releaseMatrixFromMemory() {
		
		if ( null == matrix ) return;
		
		for ( int i=0; i<matrix.length; i++ ) {
			int[] e = matrix[i];
			if ( null != e ) for ( int j=0; j<=MAX_LINKS; j++ ) e[j] = 0;
			matrix[i] = null;
		}
		matrix = null;
	}
	
	/**
	 * Expects that the given file has lines with pairs of 'linked' integers separated by commas, e.g: 12 linked to 45 would be: 12,45
	 * Integers are assumed to be in the range 0 - 10000000
	 * 
	 * The program picks out groups of integers linked to no more than MAX_LINKS other integers, then joins the result with
	 * itself to look for indirect links. Whenever 5 links are exceeded, the entities are not considered anymore.
	 * 
	 * @param args
	 * @throws IOException 
	 * @throws NumberFormatException 
	 * @throws IOException 
	 */	
	public EntityMatrixJoiner( String file, int numEntities, int maxGroupSize ) throws NumberFormatException, IOException {

		// Add 1 to numEntities so that there will always be one entity that is not connected to anyone
		// This ensures that a ResultSet full of overflows does not result in a completely empty Result which would be ambiguous
		// as it could also be interpreted as a failure to send the matrix altogether.
		numEntities++;
		
		MAX_LINKS = maxGroupSize - 1;
		
		matrix = new int[ numEntities ][ maxGroupSize ]; // 1st value is numlinks
		
		logger.logInfo("Getting groups of 1st degree links with maxGroupSize " + maxGroupSize + " from file: " + file );
		FileReader fr = new FileReader( file );
		BufferedReader br = new BufferedReader( fr );
		String line;
		
		long start = System.currentTimeMillis();
		
		int numlines=0;
		
		while( null != ( line = br.readLine() ) ) {
			
			if ( 0 == numlines++ % 10000000 )
				logger.logInfo("Loading next 10M lines from " + numlines);
			
			int cidx = line.indexOf(',');
			
			int n1 = Integer.parseInt( line.substring(0, cidx) );
			int n2 = Integer.parseInt( line.substring(cidx+1) );
			
			loadNewAssociationIntoMatrix( n1, n2 );
		}
		
		br.close();
		fr.close();
		
		long end = System.currentTimeMillis();

		logger.logInfo("Done in " + (end - start) + "ms - num distinct groups of " + MAX_LINKS + " links or less: " + numGroups);
		logger.logInfo("Max recursive overflow depth: " + maxCascadedOverflowDepth);
	}
	
	public EntityMatrixJoiner( GaianChildVTI rows, int numEntities, int maxGroupSize ) throws Exception {
		MAX_LINKS = maxGroupSize - 1;
		matrix = new int[ numEntities ][ maxGroupSize ]; // 1st value is numlinks
		
		mergeGaianChildRows( rows );
	}
	
	public int mergeGaianChildRows( GaianChildVTI rows ) throws Exception {
		
//		INIT_NUM_ENTITIES = numEntities;
//		MAX_LINKS = maxGroupSize - 1;
//		matrix = new int[ numEntities ][ maxGroupSize ]; // 1st value is numlinks
		
		logger.logInfo("Merging GaianChildRows into matrix...");
				
		long start = System.currentTimeMillis();
		
		int numlines=0;
		
		int headGroupAfterPreviousOne = 0;
		
		DataValueDescriptor[] dvdr = new DataValueDescriptor[ MAX_LINKS+2 ];
		// Create column wrappers for the 'numlinks' column and for (max entitites = MAX_LINKS+1) columns
		for ( int i=0; i<MAX_LINKS+2; i++ ) dvdr[i] = new SQLInteger();

		while( false != rows.fetchNextRow( dvdr ) ) {
			
			if ( 0 == numlines++ % 10000000 )
				logger.logInfo("Loading next 10M DVDRs from " + numlines);
			
			int numlinks = ((SQLInteger) dvdr[0]).getInt();
			int headGroup = ((SQLInteger) dvdr[1]).getInt();
			
			// NOTE: WE DON'T NEED TO DO ANYTHING ABOUT BACKWARD LINKS (i.e. when numlinks == -1)
			// BECAUSE THE INJESTION ALGO WILL RE GENERATE THEM WHEN APPROPRIATE FROM THE FORWARD LINKS
			
			// Overflow all groups that are not in the range of heads between the last one and this one.
			for ( int i=headGroupAfterPreviousOne; i<headGroup; i++ ) {
				int[] entityList = matrix[i];
				if ( MAX_LINKS < entityList[0] ) continue; // already overflowed
				overflowSubGroups( entityList ); //i );
			}
			
			headGroupAfterPreviousOne = headGroup + 1;
			
//				if ( MAX_LINKS == numlinks )
//					overflowSubGroups( matrix[ headGroup ] );
//				else
			
			// Insert all forward links in this group
			for ( int i=2; i<numlinks+2; i++ )
				loadNewAssociationIntoMatrix( headGroup, ((SQLInteger) dvdr[i]).getInt() );
		}
		
		// Overflow all groups that are not in the range of heads between the last one and the end of the matrix.
		// Only do this if we processed at least one row - this should always be the case as there is 1 more entity in the range
		// than were defined for the matrix originally - so that one should be disconnected from every other entity.
		if ( 0 < numlines )
		for ( int i=headGroupAfterPreviousOne; i<matrix.length; i++ ) {
			int[] entityList = matrix[i];
			if ( MAX_LINKS < entityList[0] ) continue; // already overflowed
			overflowSubGroups( entityList ); //i );
		}
		
		long end = System.currentTimeMillis();

		logger.logInfo("Done in " + (end - start) + "ms - num distinct groups of " + MAX_LINKS + " links or less: " + numGroups);
		logger.logInfo("Max recursive overflow depth: " + maxCascadedOverflowDepth);
		
		return numlines;
	}
	
	private void loadNewAssociationIntoMatrix( int n1, int n2 ) {
		
		int[] e1links = matrix[n1];
			
		int numlinks = e1links[0];
		
		// Whilst building these 1st degree links, we need to make sure that element heads are always back referenced.
		// So - if a sub-element links to a new element (or element head) we must make the sub-element also a sub-element to the other one
		// to avoid breaking the back reference link.
		// If both elements to be linked are sub-elements, then one of the back-references can be broken as the join can still be done from
		// the other direction. Note that when a join occurs later, all joined elements will be forced to be back referenced to the joiner.
		if ( -1 == numlinks ) {
			if ( -1 == matrix[n2][0] )
				numlinks = 0; // this element will no longer be a sub-element and become a cluster master.
			else {
				// swap the 2 entities so that the sub-element remains a sub-element.
				int n3 = n1;
				n1 = n2;
				n2 = n3;
				
				e1links = matrix[n1];			
				numlinks = e1links[0];
			}
		}
								
		if ( 0 == numlinks ) {
			numGroups++;
//			logger.logInfo("New group:\t" + n1 + "\tcount " + numGroups);
		} else {
			
			// For overflowed groups, overflow any new entites they link to
			if ( MAX_LINKS < numlinks ) {
				int[] e2links = matrix[n2];
				if ( MAX_LINKS < e2links[0] ) return; // Both are overflowed - nothing to do
				
				overflowSubGroups( e2links ); //n2 );
//				logger.logInfo("Removed subgroups of:\t" + n2 + "\tcount " + numGroups);
				return;
			}
			
			// check for duplicate association entry
			int i;
			for ( i=1; i<=numlinks; i++ )
				if ( e1links[i] == n2 ) break;
			
			if ( i<=numlinks ) return; // duplicate was found
			
			// Check that this head element is not about to overflow
			if ( MAX_LINKS == numlinks ){
				// overflow e1 itself and all subgroups - reducing numGroups in doing so
				overflowSubGroups( e1links ); //n1 );
				
				// Now ensure associated entity's links are overflowed aswell
				int[] e2links = matrix[n2];
				if ( MAX_LINKS < e2links[0] ) return; // Both are overflowed - nothing to do
				
				overflowSubGroups( e2links ); //n2 );
//				logger.logInfo("Removed subgroups of:\t" + n1 + " and " + n2 + "\tcount " + numGroups);
				return;
			}
		}
		
		e1links[0] = ++numlinks; // start at index 1 (index 0 holds the numlinks)			
		e1links[numlinks] = n2;
		
		// only make the 2nd entity a sub-element to this group 
		// if it wasnt already a cluster master or sub-element to another group 
		int[] e2links = matrix[n2];
		if ( 0 == e2links[0] ) {
			e2links[0] = -1; // denotes a back ref link
			e2links[1] = n1; // ...to n1
		}		
	}
	
	public void processJoins() {
		processJoins( MAX_LINKS+1 );
	}
	
	public void processJoins( int numJoins ) {
		
		remainingGroups = null;
		int[] previousRemaining = null;
		
		for ( int i=0; i<numJoins; i++ ) {
			
			if ( i < 2 ) showMatrix(10);
			
			maxCascadedOverflowDepth = 0;
			cascadedOverflowDepth = 0;
			
			logger.logInfo("Joining matrix with itself and removing overflowed groups, join iteration #" + (i+1));
			
			long start = System.currentTimeMillis();
		
			// this will be the previously remaining entities before the join was applied.
			remainingGroups = new int[ numGroups ];
			joinMatrix( remainingGroups, previousRemaining );
			
			previousRemaining = remainingGroups;
			
			long end = System.currentTimeMillis();

			logger.logInfo("Done in " + (end - start) + "ms - num distinct groups of " + MAX_LINKS + " or less: " + numGroups);
			logger.logInfo("Max recursive overflow depth: " + maxCascadedOverflowDepth);
			
//			showMatrix(matrix, remaining, 50);
		}
		
		showMatrix(10);
		
//		if ( null != remaining )
//		showMatrix(matrix, remaining, 50);
		
		linkCounts = new int[MAX_LINKS];
		for (int i=0; i<remainingGroups.length; i++) {
			int count = matrix[ remainingGroups[i] ][0];
			if ( 0 < count && count <= MAX_LINKS ) linkCounts[ count-1 ]++;
		}
		
		logger.logInfo("Number of groups for each links size category [1-" + MAX_LINKS + "]: " + Arrays.toString( linkCounts ) );
		
		logger.logInfo("Sample Results (2 of each group):");
		for ( int i=0; i<MAX_LINKS; i++ ) showMatrixResults( i, 2 );
		
		// Initialise the set of restricted sized groups to the whole set of groups
		restrictedSizeGroupsRemaining = remainingGroups;
	}

	private void overflowSubGroups( int[] entityList ) {
		
		// entityList[0] is assumed to have already overflowed		
		cascadedOverflowDepth++;
		
		int numlinks = entityList[0];
		if ( 0 < numlinks ) numGroups--;
		entityList[0] = MAX_LINKS+1;
		
		// When dealing with backwards refs, there is only one link to overflow.
		if ( -1 == numlinks ) numlinks = 1;
		
		for ( int i=1; i<=numlinks; i++ ) {
			
//			try {
			int[] sublist = matrix[ entityList[i] ];			
//			logger.logInfo("i = " + i + ", entityList[i] = " + entityList[i] + ":\t" + Arrays.toString( sublist ) );
			
			if ( MAX_LINKS >= sublist[0] ) {
				
				overflowSubGroups( sublist );
				
				if ( cascadedOverflowDepth > maxCascadedOverflowDepth ) maxCascadedOverflowDepth = cascadedOverflowDepth;
			}
//			}
//			catch (ArrayIndexOutOfBoundsException e) {
//
//				logger.logException("Exception in overflowSubGroups: i=" + i, e);
//				logger.logException("Exception in overflowSubGroups: i=" + i + ", entityList[i]=" + entityList[i], e);
//				throw e;
//			}
		}
		
		cascadedOverflowDepth--;
	}
	
//	private void overflowSubGroups( int eindex ) {
//		
//		cascadedOverflowDepth++;
//		
//		int[] entityList = matrix[ eindex ];
//		int numlinks = entityList[0];
//		if ( 0 < numlinks ) numGroups--;
//		entityList[0] = MAX_LINKS+1;
//		
//		// When dealing with backwards refs, there is only one link to overflow.
//		if ( -1 == numlinks ) numlinks = 1;
//		
//		for ( int i=1; i<=numlinks; i++ ) {
//			
//			int e2index = entityList[i];
//			
////			try {
//			int[] sublist = matrix[ e2index ];			
////			logger.logInfo("i = " + i + ", entityList[i] = " + entityList[i] + ":\t" + Arrays.toString( sublist ) );
//			
//			int numlinks2 = sublist[0];
//			
//			if ( MAX_LINKS < numlinks2 ) continue;
//			
//			if ( MAX_LINKS > numlinks2 ) {
//				
//				sublist[ -1 == numlinks2 ? 1 : numlinks2+1 ] = eindex;
//				continue;
//			}
//			
//			overflowSubGroups( e2index );
//			
//			if ( cascadedOverflowDepth > maxCascadedOverflowDepth ) maxCascadedOverflowDepth = cascadedOverflowDepth;
//			
////			}
////			catch (ArrayIndexOutOfBoundsException e) {
////
////				logger.logException("Exception in overflowSubGroups: i=" + i, e);
////				logger.logException("Exception in overflowSubGroups: i=" + i + ", entityList[i]=" + entityList[i], e);
////				throw e;
////			}
//		}
//		
//		cascadedOverflowDepth--;
//	}
	
	// Future optimization: only re-join against the newly added entities in each remaining group
	private void joinMatrix( int[] remaining, int[] previousRemaining ) {
		
		int idx = 0;
		boolean isFirstPass = null == previousRemaining;
		int max =  isFirstPass ? matrix.length : previousRemaining.length;
		
		// Apply the join and figure out which entities were previously remaining
		for ( int e1 = 0; e1<max; e1++ ) {
			
			int newEntityIndex = isFirstPass ? e1 : previousRemaining[e1];
			int[] e1links = matrix[ newEntityIndex ];
			if ( null == e1links ) continue; // No entities are linked to this entity
			int numlinks = e1links[0];
//			if ( 0 == numlinks ) { matrix[ newEntityIndex ] = null; continue; }
			if ( 0 >= numlinks || MAX_LINKS < numlinks ) continue;
			
			remaining[ idx++ ] = newEntityIndex;
			
			for ( int j=1; j<=numlinks; j++ ) {
				
				int e2 = e1links[j];
				int[] e2links = matrix[ e2 ];
				
//				logger.logInfo("NumGroups " + numGroups + ", Processing " + 
//						newEntityIndex + ": " + Arrays.toString( e1links ) + " with " + e2 + ": " + Arrays.toString( e2links ));
				
				int numlinks2 = e2links[0];
				boolean isOverflow = false;
				
				if ( 0 == numlinks2 ) {
					logger.logInfo("Error: unexpected 0 count for referenced entity - should be -1 (back ref) or positive");
					continue;
				}
				
				// If we have hit an element which is a back referenced sub-element, then add the head element.
				// future extn: switch e2 to it to process all its links ? (this wd require more checks on overflows)
				if ( -1 == numlinks2 ) {
					
					int e3 = e2links[1]; // the back reference element in e2's list to join to the list of e1's
					if ( e3 != newEntityIndex ) isOverflow = !addEntitySkipDuplicates( e1links, e3 );
				
				} else {
				
					if ( MAX_LINKS < numlinks2 ) {
						
						isOverflow = true;
						
					} else for ( int k=1; k<=numlinks2; k++ ) { // Add all elements from e2links into e1links - stop if overflow occurs
						
						int e3 = e2links[k]; // the next element in e2's list to join to the list of e1's
						
//						logger.logInfo("e1: " + e1 + ", e3 " + e3);

						// Ensure the element from e2links to be added to e1links is not equal to its head-element!
						if ( e3 != newEntityIndex ) {
							isOverflow = !addEntitySkipDuplicates( e1links, e3 );
							if ( isOverflow ) break; // stop processing these overflowed links
						}
					}
				}
				
				if ( isOverflow ) {
					// Overflow e1links and all its sub-elements' links recursively
					overflowSubGroups( e1links ); //newEntityIndex );
					break; // stop processing these overflowed links
				}
				
				// We didnt overflow - make the group for this e2links element empty now as its been joined to e1links.
				// Also, back reference it to the e1links head so that other joins to this element will be redirected it.
				if ( -1 != numlinks2 ) {
					numGroups--;
					e2links[0] = -1;
				}
				
				e2links[1] = newEntityIndex;
			}
		}
	}
		
	/**
	 * Adds entity to entities array, unless it is already in it.
	 * Returns true if overflow occurs.
	 */
	private boolean addEntitySkipDuplicates( int[] entities, int entity ) {

//		logger.logInfo("Trying to add " + entity + " to " + Arrays.toString( entities ));
		
		int numlinks = entities[0];
		for ( int i=1; i<=numlinks; i++ )
			if ( entities[i] == entity ) return true;
		
		if ( MAX_LINKS == numlinks ) return false;
		
		entities[0] = ++numlinks;
		entities[numlinks] = entity;
		return true;
	}
	
//	public void setGroupSizeRestriction( int maxGroupSize ) {
//		
//		int numRestrictedSizeGroups = 0;
//		int maxLinks = maxGroupSize-1;
//		
//		// example: group size of 2 -> links size of 1 -> only pick linkCounts[0]
//		for ( int i=0; i<maxLinks; i++ ) numRestrictedSizeGroups += linkCounts[i];
//		
//		restrictedSizeGroupsRemaining = new int[ numRestrictedSizeGroups ];
//		int restrictedGroupsIdx = 0;
//		
//		for ( int i=0; i<remainingGroups.length; i++ ) {
//			int groupIdx = remainingGroups[i];
//			int numLinks = matrix[ groupIdx ][0];
//			if ( 0 < numLinks && maxLinks >= numLinks )
//				restrictedSizeGroupsRemaining[restrictedGroupsIdx++] = groupIdx;
//		}
//	}
	
	public void writeNonOverflowedRowsToFile( String fileName ) throws IOException {
		
		BufferedWriter bw = new BufferedWriter( new FileWriter(fileName) );
		
		StringBuffer sb = new StringBuffer();
		
		// Strings to complete packed zero ranges -
		// Corresponds to the packing required at the end of a row after zero-range is written
		String zeroPack = new String();
		for ( int i=1; i<MAX_LINKS; i++ ) zeroPack += ",";
		
		for ( int i=0; i<matrix.length; i++ ) {
			int[] entryList = matrix[i];
			if ( null == entryList ) continue;
			if ( MAX_LINKS < entryList[0] ) continue;
			int numlinks = entryList[0];
			
			sb.setLength(0);
			sb.append( numlinks );
			sb.append(',');
			sb.append( i );
			
//			if ( 0 == numlinks ) {
//				int j = i;
//				for ( ; i+1<matrix.length; i++ ) {
//					int[] entryList2 = matrix[i+1];
//					if ( null == entryList2 || 0 != entryList2[0] ) break;
//				}
//				
//				sb.append(',');
//				if ( i != j ) sb.append( i );
//				sb.append( zeroPack );					
//				
//				bw.write( sb.toString() );
//				bw.newLine();
//				continue;
//			}
			
			int j=1;
			for ( ; j<numlinks+1; j++ ) {
				sb.append(','); sb.append( entryList[j] );
			}
			
			for ( ; j<MAX_LINKS+1; j++ ) sb.append(',');
			
			bw.write( sb.toString() );
			bw.newLine();
			
//			bw.write( Integer.toString( numlinks ) );
//			bw.write(',');
//			bw.write( Integer.toString( i ) );
//			for ( int j=1; j<MAX_LINKS; j++ ) {
//				bw.write(','); bw.write( Integer.toString( entryList[j] ) );
//			}
//			bw.newLine();
		}
		
		bw.close();
	}
	
	public void setGroupSizeRestriction( int requestedGroupSize ) {
		
		int requestedLinks = requestedGroupSize-1;
		
		// example: group size of 2 -> links size of 1 -> only pick linkCounts[0]
		
		restrictedSizeGroupsRemaining = new int[ linkCounts[ requestedLinks-1 ] ];
		int restrictedGroupsIdx = 0;
		
		for ( int i=0; i<remainingGroups.length; i++ ) {
			int groupIdx = remainingGroups[i];
			int numLinks = matrix[ groupIdx ][0];
			if ( requestedLinks == numLinks )
				restrictedSizeGroupsRemaining[restrictedGroupsIdx++] = groupIdx;
		}
	}
	
	public int getNumGroups() {
		return restrictedSizeGroupsRemaining.length;
	}

	public int getGroupHead( int idx ) {
		return restrictedSizeGroupsRemaining[idx];
	}
	
	public int[] getGroupRow( int headIdx ) {
		return matrix[ headIdx ];
	}
	
	private void showMatrix( int max ) {
		
//		int[] exceptions = new int[] { 518, 8130874, 2972626, 19686, 665417, 1252944 };
//		for ( int i=0; i<exceptions.length; i++ ) {
//			logger.logInfo( "EntityLinks(" + exceptions[i] + ") = " + Arrays.toString( matrix[ exceptions[i] ] ) );
//		}
		
		int trueGroupCount = 0, brc = 0, ovfc = 0, nlc = 0;
		
		for ( int i=0; i<matrix.length; i++ ) {
//			if ( null == matrix[i] ) { nlc++; continue; }
			int numlinks = matrix[i][0];
			if ( numlinks > 0 && numlinks <= MAX_LINKS )
				trueGroupCount++;
			if ( 0 == numlinks ) nlc++;
			if ( -1 == numlinks ) brc++;
			if ( MAX_LINKS < numlinks ) ovfc++;
		}
		
		logger.logInfo("Real Distinct Groups Count: " + trueGroupCount);
		logger.logInfo("Back References Count: " + brc);
		logger.logInfo("Overflowed Entities Count: " + ovfc);
		logger.logInfo("Non-Linked Entities Count: " + nlc);
		
		for ( int i=0; i<matrix.length && 0 < max; i++ ) {
			int[] entryList = matrix[i];
			if ( null == entryList ) continue;
			int numlinks = entryList[0];
			if ( numlinks > 0 && numlinks <= MAX_LINKS ) {
				max--;
				logger.logInfo( "EntityLinks(" + i + ") = " + Arrays.toString( matrix[ i ] ) );
			}
		}
	}
	
//	private void showMatrixNonLinkedElements( int max ) {
//		showMatrixResults( 0, max );
//	}
	
	private void showMatrixResults( int groupSize, int max ) {
		
		if ( 0 == groupSize ) return; // Empty groups are not kept
		
		for ( int i=0; i<matrix.length && 0 < max; i++ ) {
			int[] entryList = matrix[i];
			if ( null == entryList ) continue;
			int numlinks = entryList[0];
			if ( groupSize == numlinks ) {
				max--;
				logger.logInfo( "EntityLinks(" + i + ") = " + Arrays.toString( matrix[ i ] ) );
			}
		}		
	}
	
//	private void showMatrix( int[] remaining ) {
//		showMatrix( remaining, remaining.length );
//	}
	
//	private void showMatrix( int[] remaining, int max ) {
//		
//		if ( max > remaining.length ) max = remaining.length;
//		
//		for ( int i=0; i<max; i++ ) {
//			int idx = remaining[i];
//			if ( null == matrix[ idx ] ) continue;
//			logger.logInfo( "EntityLinks(" + idx + ") = " + Arrays.toString( matrix[ idx ] ) );
//		}
//	}
	
//	private static boolean isIn( int a, int[] list ) {
//		for ( int i=0; i<list.length; i++ )
//			if ( a == list[i] ) return true;
//		return false;
//	}
//	
//	private static Object resizeArray (Object oldArray, int newSize) {
//		   int oldSize = java.lang.reflect.Array.getLength(oldArray);
//		   Class elementType = oldArray.getClass().getComponentType();
//		   Object newArray = java.lang.reflect.Array.newInstance(
//		         elementType,newSize);
//		   int preserveLength = Math.min(oldSize,newSize);
//		   if (preserveLength > 0)
//		      System.arraycopy (oldArray,0,newArray,0,preserveLength);
//		   return newArray;
//	}
}
