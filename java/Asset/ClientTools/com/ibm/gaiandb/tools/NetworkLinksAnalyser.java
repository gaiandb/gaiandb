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

import com.ibm.gaiandb.apps.dashboard.TopologyGraph.Edge;

public class NetworkLinksAnalyser {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

//	int INIT_NODES = 100;
//	int MAX_STEPS = 20;
	
	// Hashtable of node names
	private final Hashtable<String, Set<String>> links = new Hashtable<String, Set<String>>(); //NUM_NODES);
	
	private final Hashtable<String, Set<String>> furthestNodes = new Hashtable<String, Set<String>>(); //NUM_NODES);
	
	// Hashtables of ArrayLists
	private final Hashtable<String, ArrayList<Integer>> stepCardinalities = 
		new Hashtable<String, ArrayList<Integer>>(); //NUM_NODES); //[MAX_STEPS+1];
	
	private final Hashtable<String, ArrayList<Integer>> runningTotalCardinalities = 
		new Hashtable<String, ArrayList<Integer>>(); //NUM_NODES); //[MAX_STEPS+1];

	private int radius = -1, diameter = 0;
	private int[] nodesPerEccentricity = null, nodesPerConnectivity = null;

	
	private static final String delimiter = "	";

	public static void main( String[] args) throws Exception {
		
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\GDB52-emj.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\GDB120-propnewalgo.txt"; //GDB120-propnolimit.txt"; //GDB120-prop.txt"; 
		String filename = args.length > 0 ? args[0] : "C:\\temp\\gdb1250.txt";
//		String filename = args.length > 0 ? args[0] : "C:\\temp\\gdb120fix.txt";
		
		NetworkLinksAnalyser nla = new NetworkLinksAnalyser();
		
		FileReader fr = new FileReader( filename );
		BufferedReader br = new BufferedReader( fr );
		String line;
		
		while( null != ( line = br.readLine() ) ) {
			
			int cidx = line.indexOf( delimiter );
			
			if ( -1 == cidx ) continue;
			
			// connected nodes n1 and n2
			String n1 = line.substring(0, cidx);
			String n2 = line.substring(cidx+delimiter.length());
			nla.addLink(n1, n2);
		}
		
		nla.computeAnalysis();
	}

	public void computeStats( Set<Edge> topologyGraphEdges ) {
		
		links.clear();
		stepCardinalities.clear();
		runningTotalCardinalities.clear();
		furthestNodes.clear();
		
		radius = -1; diameter = 0;
		
		for ( Edge edge : topologyGraphEdges )
			addLink(edge.source.getName(), edge.target.getName());
		
		long time = -System.currentTimeMillis();
		computeAnalysis();
		time += System.currentTimeMillis();
//		System.out.println("Computed Analysis in millis: " + time);
	}
	
	private void computeAnalysis() {
		
		Set<String> visited = new HashSet<String>();
		
		int maxNodeConnections = 0;
		
		Iterator<String> nodes = links.keySet().iterator();
		while(nodes.hasNext()) {
			String node = nodes.next();
			
			visited.clear();
			Set<String> previouslyVisited = new HashSet<String>();
			
			// for every step out
			for ( int s=0; /*s<MAX_STEPS*/; s++ ) {
				
				Set<String> newlyVisited = new HashSet<String>();
				
				if ( visited.isEmpty() ) {
					// Just add ourself at depth step 0
					newlyVisited.add( node );
				} else {
					
					// Compute cardinality at next step by getting all links m of the newly visited nodes
//					Iterator<String> iter = previouslyVisited.iterator();
//					while( iter.hasNext() ) {
//						String m = iter.next();
//						newlyVisited.addAll( links.get(m) );
//					}
					
					for( String m : previouslyVisited )
						newlyVisited.addAll( links.get(m) );
					
					newlyVisited.removeAll( visited );
				}
				
				// Early end condition: No new nodes visited
				if ( 0 == newlyVisited.size() ) break;
				
				visited.addAll( newlyVisited );				

				previouslyVisited = newlyVisited;
				
				if ( 0 == s ) continue;
				
				int numNewlyVisited = newlyVisited.size();				
				if ( 1 == s ) maxNodeConnections = Math.max( maxNodeConnections, numNewlyVisited );
				
				stepCardinalities.get(node).add( numNewlyVisited );
				runningTotalCardinalities.get(node).add( visited.size() );
			}
			
			if ( null != previouslyVisited ) furthestNodes.put(node, previouslyVisited);
			
//			if ( stepCardinalities[n].size()-1 > diameter ) diameter = stepCardinalities[n].size()-1;
			if ( stepCardinalities.get(node).size() > diameter )
				diameter = stepCardinalities.get(node).size();
			
			if ( -1 == radius || stepCardinalities.get(node).size() < radius )
				radius = stepCardinalities.get(node).size();
		}
		
		// print results
//		System.out.println("Network Radius: " + radius + ", Diameter: " + diameter);
		
//		System.out.println("\nStep Cardinalities Per Node, i.e. number of newly connected nodes at each step out, for every node:");
//		System.out.println("Note: Nodes are sorted by the distance they are from their furtest nodes:\n"); // / Running Totals
		
		nodesPerEccentricity = new int[ diameter ];
		
		for ( int s=1; s<diameter+1; s++ ) {
			
			for( String node : stepCardinalities.keySet() ) {
				ArrayList<Integer> nodeStepCardinalities = stepCardinalities.get( node );
				if ( nodeStepCardinalities.size() == s) {
					nodesPerEccentricity[s-1]++;
					int total = 0;
					for ( int i=0; i<nodeStepCardinalities.size(); i++ ) {
						total += (Integer) nodeStepCardinalities.get(i);
					}
//					System.out.println("Node \t" + node + " (total: " + total + "):\t" + nodeStepCardinalities ); //+ " / " + runningTotalCardinalities[n] );
				}
			}			
		}
		
		nodesPerConnectivity = new int[maxNodeConnections];
		if ( 0 < maxNodeConnections )
			for( String node : stepCardinalities.keySet() ) {
				ArrayList<Integer> nodeStepCardinalities = stepCardinalities.get( node );
				if ( 0 < nodeStepCardinalities.size() )
					nodesPerConnectivity[nodeStepCardinalities.get(0)-1]++;
			}
		
//		System.out.println("\nDiameter cardinalities: Number of nodes reaching out to all other nodes for each diameter size up to max diameter:");
//		System.out.println("This is a measure of the Centeredness of nodes / Compactness of the network");
//		System.out.println("Number of nodes for each diameter size: " + intArrayAsString(diameterCardinalities));
	}
	
	private void addLink( String node1, String node2 ) {

		// Populate links and cardinalities for node1
		Set<String> s = links.get(node1);
		if ( null == s ) {
			s = new HashSet<String>();
			links.put(node1, s);
			stepCardinalities.put(node1, new ArrayList<Integer>());
			runningTotalCardinalities.put(node1, new ArrayList<Integer>());
		}
		s.add(node2);

		// Populate links and cardinalities for node2	
		s = links.get(node2);
		if ( null == s ) {
			s = new HashSet<String>();
			links.put(node2, s);
			stepCardinalities.put(node2, new ArrayList<Integer>());
			runningTotalCardinalities.put(node2, new ArrayList<Integer>());
		}
		s.add(node1);
	}
	
	public static String intArrayAsString(int[] a) {
		if ( null==a ) return null; int len = a.length;
		String pcs = new String( 0<len ? "[" + a[0] : "[" );
//		for (int i=1; i<len; i++) pcs += ", " + a[i]; pcs += "]";
		for (int i=1; i<len; i++) pcs += ", " + a[i]; pcs += "]";
		return pcs;
	}
	
	public int getEccentricity( String node ) {
		ArrayList<Integer> steps = stepCardinalities.get(node);
		if ( null == steps ) return 0;
		return steps.size();
	}
	
	public String getNodesPerEccentricity() {
		return intArrayAsString(nodesPerEccentricity);
	}
	
	public String getNodesPerConnectivity() {
		return intArrayAsString(nodesPerConnectivity);
	}
	
	public int getNumConnections( String node ) {
		ArrayList<Integer> steps = stepCardinalities.get(node);
		if ( null == steps || 1 > steps.size() ) return 0;
		return steps.get(0);
	}
	
	public ArrayList<Integer> getStepCardinalities( String node ) {
		return stepCardinalities.get(node);
	}
	
	public Set<String> getFurthestNodes(String node) {
		return furthestNodes.get(node);
	}

	public int getDiameter() {
		return diameter;
	}

	public int getRadius() {
		return radius;
	}
}
