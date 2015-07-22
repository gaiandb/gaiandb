/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.searchapis;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Vector;

import org.apache.derby.iapi.types.DataValueDescriptor;

public class TestSIAPI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	public void execute() {
		String portNumber = "80";
		String searchString = "tony blair";
		String collections = "SQLServerCatsa";
//		String collectionName = "";
//		String collectionId = "SSCatsa";
		String applicationName = "esadmin";
		String applicationPassword = "kingle4r";	
		String hostname = "lmbhm1b";
		int maxRows = 1000000;
//		String hostname = "9.20.147.205";
		
		Vector<DataValueDescriptor[]> resultRows = new Vector<DataValueDescriptor[]>();

//		getDocuments.printTest();

		SearchSIAPI.retrieveDocumentReferences(resultRows,hostname,portNumber,collections,searchString,
				maxRows,applicationName,applicationPassword);
		int icount = 0;
		Iterator<DataValueDescriptor[]> i = resultRows.iterator();
		while ( i.hasNext() ) {
		System.out.println("Result Row: " + Arrays.asList(i.next()));
		icount++;
		
		}
		System.out.println("Num Rows: " + icount);
	}

	public static void main(String[] args) throws Exception {

		TestSIAPI df = new TestSIAPI();
		df.execute();
	}
}
