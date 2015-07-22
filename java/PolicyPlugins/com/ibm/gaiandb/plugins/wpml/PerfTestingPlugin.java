/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml;

import com.ibm.watson.pml.PMLException;
import com.ibm.watson.pml.pep.IObjectPEP;
import com.ibm.watson.pml.pep.ObjectPEP;

/**
 * @author dawood@us.ibm.com
 *
 */
public class PerfTestingPlugin extends PolicyEnabledFilter {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	/**
	 * 
	 */
	public PerfTestingPlugin() {
		super();
	}
	
	/**
	 * For performance testing, we use a single  PEP that always
	 * returns true, if no policies apply.
	 */
	protected static IObjectPEP allocateObjectPEP() throws PMLException {
		 return new ObjectPEP("pfg-pep");
	}
}
