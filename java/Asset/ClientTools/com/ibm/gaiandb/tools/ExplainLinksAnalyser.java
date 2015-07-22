/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

public class ExplainLinksAnalyser {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
//	private static final String delimiter =" ";
	
	private static Vector<Vector<String>> longestRoutes = new Vector<Vector<String>>();

	public static void main( String[] args) throws Exception {
		
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\GDB52-emj.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\GDB120-propnewalgo.txt"; //GDB120-propnolimit.txt"; //GDB120-prop.txt"; 
		String filename = args.length > 0 ? args[0] : "C:\\temp\\gdb520.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\gdb120fix.txt";

		
//		int INIT_NODES = 100;
//		int MAX_STEPS = 20;
		
		// Hashtable of node names
		Hashtable<String, Set<String>> links = new Hashtable<String, Set<String>>(); //NUM_NODES);
		
		// Hashtables of ArrayLists
		Hashtable<String, ArrayList<Integer>> stepCardinalities = new Hashtable<String, ArrayList<Integer>>(); //NUM_NODES); //[MAX_STEPS+1];
		Hashtable<String, ArrayList<Integer>> runningTotalCardinalities = new Hashtable<String, ArrayList<Integer>>(); //NUM_NODES); //[MAX_STEPS+1];
		
		String queryStartNode = "";
		
		FileReader fr = new FileReader( filename );
		BufferedReader br = new BufferedReader( fr );
		String line;
		
		while( null != ( line = br.readLine() ) ) {
			
			int cidx = line.indexOf( " -> " );
			
			if ( -1 == cidx ) continue;
			
			// before cidx is a query source in quotes, i.e. "hipods095.hursley.ibm.com:6415"
			String sourceNode = line.substring (1,cidx-1);
			
			// after cidx is a query destination in quotes, i.e. "hipods095.hursley.ibm.com:6415"
			
			int tokenEnd=line.indexOf( "\"", cidx+5);
			String destinationNode = line.substring (cidx+5,tokenEnd);

			// after the destination is square bracketed style marker - showing whether the query was executed, or a duplicate
			// such as [style=dotted] or  [label=0];
			// connected nodes n1 and n2
			
			int tokenStart=line.indexOf("[");
			tokenEnd=line.indexOf("=",tokenStart);
			//String labelType=line.substring(tokenStart+1,tokenEnd);

			// Get second half of the label
			tokenStart=tokenEnd+1;
			tokenEnd=line.indexOf("]",tokenStart);
			//String labelData = line.substring(tokenStart,tokenEnd);

		
			//String n1 = line.substring(0, cidx);
			//String n2 = line.substring(cidx+delimiter.length());
			
			String sub = sourceNode.substring(0,13);
			if (sub.equals("SQL Query on ")) {
				 queryStartNode=destinationNode;
				continue;
			}
			
			
			Set<String> s = links.get(sourceNode);
			if ( null == s ) {
				s = new HashSet<String>();
				links.put(sourceNode, s);
				stepCardinalities.put(sourceNode, new ArrayList<Integer>());
				runningTotalCardinalities.put(sourceNode, new ArrayList<Integer>());
			}
			s.add(destinationNode);
			
			s = links.get(destinationNode);
			if ( null == s ) {
				s = new HashSet<String>();
				links.put(destinationNode, s);
				stepCardinalities.put(destinationNode, new ArrayList<Integer>());
				runningTotalCardinalities.put(destinationNode, new ArrayList<Integer>());
			}
			s.add(sourceNode);
		}
		
		Set<String> visited = new HashSet<String>();
		
		// find the querysource node
		String node = queryStartNode;
		
		
		// Go through nodes one at a time to calculate their distance to all other nodes.
		//Iterator nodes = links.keySet().iterator();
		//while(nodes.hasNext()) {
//			String node = (String) nodes.next();
			
			visited.clear();
			Set<String> previouslyVisited = null;
			
		    // At each step out, keep the list on newly visited nodes.
			Hashtable<Integer, Set<String>> newlyvisitedPerStep = new Hashtable<Integer, Set<String>>();
			
			// for every step out
			for ( int s=0; /*s<MAX_STEPS*/; s++ ) {
				
				Set<String> newlyVisited = new HashSet<String>();
				
				if ( visited.isEmpty() ) {
					// Just add ourself at depth step 0
					newlyVisited.add( node );
				} else {
					
					// Compute cardinality at next step by getting all links m of the newly visited nodes
					Iterator<String> iter = previouslyVisited.iterator();
					while( iter.hasNext() ) {
						String m = iter.next();
						newlyVisited.addAll( links.get(m) );
					}
					newlyVisited.removeAll( visited );
				}
				
				// Early end condition: No new nodes visited
				if ( 0 == newlyVisited.size() ) break;
				
				visited.addAll( newlyVisited );
				stepCardinalities.get(node).add( newlyVisited.size() );
				runningTotalCardinalities.get(node).add( visited.size() );
				
				newlyvisitedPerStep.put(new Integer(s), newlyVisited);
				
				// Early end condition: All nodes visited
//				if ( NUM_NODES == visited.size() ) break;
						
				previouslyVisited = newlyVisited;
			}
			
			// use a recursive algorithm to step back and find the proutes to the longest points.
			Iterator<String> visitIter = previouslyVisited.iterator();
			int numberOfSteps = newlyvisitedPerStep.size();

			while( visitIter.hasNext() ) {
				String m = visitIter.next();
				Vector<String> nodeRoute = new Vector<String>();
				nodeRoute.add(m);
				WorkOutRoute(numberOfSteps, nodeRoute, newlyvisitedPerStep, links);

			}
			
			for (Vector<String> longestRoute : longestRoutes) {
				System.out.print ("Longest Route: ");
				for (Object routeStep : longestRoute) {
					Set<String> linkSet = links.get((String)routeStep);
					int numberOfLinks = linkSet.size();
					System.out.print((String)routeStep + ", connections: " + numberOfLinks + ", ");
				}
				System.out.println("");
				
			}
			//for ( int s=0; /*s<MAX_STEPS*/; s++ ) {			
			//int numberOfSteps = newlyvisitedPerStep.size();			
			//for ( int step=step; s==0; s-- ) {

			
			// At the point when we exit, previouslyVisited holds the set of the furthest out nodes.
			// Iterate through these and work backwards to find the shortest route to them.
			//numberOfSteps = newlyvisitedPerStep.get(key)
			//for 
			
	}

	// Recursively work back through the newlyvisitedPerStep data and the links to find instances of the route within the
	// number of steps given, Accrete the routes by inserting into the nodeRoute.
    private static void WorkOutRoute(int numberOfSteps, Vector<String> nodeRoute,
			Hashtable<Integer, Set<String>> newlyvisitedPerStep, Hashtable<String, Set<String>> links) {
		String node = (String) nodeRoute.get(0);
		
		Set<String> nodeConnections = links.get(node);
		//reduce the links to just those the right number of steps form the source
		Set<String> nodesAtCorrectStep = newlyvisitedPerStep.get(new Integer(numberOfSteps-2));
		nodesAtCorrectStep.retainAll(nodeConnections);
		
		
		for (String previousNode : nodesAtCorrectStep) {
		    @SuppressWarnings("unchecked")
			Vector<String> extendedNodeRoute=(Vector<String>)nodeRoute.clone();
			extendedNodeRoute.add(0, previousNode);
			if (numberOfSteps>2)
			WorkOutRoute(numberOfSteps-1,extendedNodeRoute,newlyvisitedPerStep,links);
			else
				//We have got back to the beginning with a full valid maximum length route.
				longestRoutes.add(extendedNodeRoute);
			
		}
		
	}
	
//	public static String intArrayAsString(int[] a) {
//		if ( null==a ) return null; int len = a.length;
//		String pcs = new String( 0<len ? "[" + a[0] : "[" );
////		for (int i=1; i<len; i++) pcs += ", " + a[i]; pcs += "]";
//		for (int i=1; i<len; i++) pcs += ", " + a[i]; pcs += "]";
//		return pcs;
//	}
}
