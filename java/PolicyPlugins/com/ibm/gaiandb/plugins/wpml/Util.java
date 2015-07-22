/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml;

import java.io.File;
import java.io.IOException;

import com.ibm.watson.pml.PMLException;
import com.ibm.watson.pml.pdp.IPolicyDecisionPoint;
import com.ibm.watson.pml.repository.PolicyRepositoryUtility;

/**
 * Utility functions for searching and loading policies into a PDP
 * 
 * @author pzerfos@us.ibm.com
 *
 */
public class Util {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static String policiesFilename = null;
	
	private static final String WPML_PFG_POLICIES_FILE_ENV = "WPML_PFG_POLICIES_FILE";
	private static final String DEFAULT_PFG_POLICIES_FILENAME = "C:\\PFGpolicies.spl";
	
	/**
	 * Searches for existence of a file that contains policies. It searches in order:
	 * <ol>
	 * <li> The supplied policiesFilename parameter
	 * <li> The default environment variable  {@value #WPML_PFG_POLICIES_FILE_ENV}
	 * <li> The default file name {@value #DEFAULT_POLICIES_FILENAME}
	 * </ol>
	 * 
	 * @param userPoliciesFilename a String with a file that contains SPL policies
	 * 
	 * @return <code>true</code> if the policies file has been found, <code>false</code> otherwise
	 */
	public static boolean searchForPoliciesFile(String userPoliciesFilename) {
		// 1. Check supplied parameter
		//System.out.println("PolicyEnabledSFDiscoAgent: searching policies in filename: " + userPoliciesFilename);
		if (existsPoliciesFile(userPoliciesFilename)) {
			System.out.println("PFG: loading policies from file: " + userPoliciesFilename);
			policiesFilename = userPoliciesFilename;
			return true;
		}
		
		// 2.  Check environment variable
		String policiesFile = System.getenv(WPML_PFG_POLICIES_FILE_ENV);
		if (policiesFile != null && existsPoliciesFile(policiesFile)) {
			System.out.println("PFG: loading policies from file found from env. properties: " + WPML_PFG_POLICIES_FILE_ENV);
			policiesFilename = policiesFile;
			return true;
		}
		
		// 3. Check default location
	    if (existsPoliciesFile(DEFAULT_PFG_POLICIES_FILENAME)) {
	    	System.out.println("PFG: loading policies from default file: " + DEFAULT_PFG_POLICIES_FILENAME);
			policiesFilename = DEFAULT_PFG_POLICIES_FILENAME;
			return true;
		}
	    
	    return false;
	}
	
	/**
	 * Check for existence of a file.
	 * 
	 * @param policiesFilename a String with the name of the file to be checked for existence
	 * @return <code>true</code> if file was found, <code>false</code> otherwise or if file name was <code>null</code>
	 */
	private static boolean existsPoliciesFile(String policiesFileName) {
		// If filename is null, simply return
		if (policiesFileName == null)
			return false;
		
		// Test for existence of file with the given file name
		File f = new File(policiesFileName);
		if (f.exists())
			return true;
		
		return false;
	}
	
	public static String getPoliciesFileName() {
		return policiesFilename;
	}
	
	/**
	 * Load the policies from the given file into the repository
	 * 
	 * @param policyFile
	 * @param pdp
	 * @throws PMLException
	 * @throws IOException
	 */
	public static void loadPolicies(String policyFile, IPolicyDecisionPoint pdp) throws PMLException, IOException {
		// Be sure the repository is connected before accessing it.
		pdp.connect();
		
		// Because an IPolicyDecisionPoint is also an IPolicyRepository,
		// we can pass it to the import method here.  Note that usually,
		// policies will be deployed into a PDP via an IPolicyManager
		PolicyRepositoryUtility.importPolicies(policyFile, pdp);
	}
	
	/**
	 * Load the policie from the given file into a repository after clearing it from pre-existing
	 * policies.
	 * 
	 * @param policyFile
	 * @param pdp
	 * @throws PMLException
	 * @throws IOException
	 */
	public static void clearLoadPolicies(String policyFile, IPolicyDecisionPoint pdp) throws PMLException, IOException {
		// Be sure the repository is connected before accessing it
		pdp.connect();
		
		// Clear the repository from previous policies
		pdp.clear();
		
		// Because an IPolicyDecisionPoint is also an IPolicyRepository,
		// we can pass it to the import method here.  Note that usually,
		// policies will be deployed into a PDP via an IPolicyManager
		PolicyRepositoryUtility.importPolicies(policyFile, pdp);
	}
}
