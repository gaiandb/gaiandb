/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.lite;

import java.sql.SQLException;
import org.apache.derby.vti.IFastPath;
import com.ibm.gaiandb.GaianResultSetMetaData;

public interface TableResultExtractor extends IFastPath {
	
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";

	public GaianResultSetMetaData getTableMetaData() throws SQLException;
	public void setResultColumns(int[] resultColumns);
}
