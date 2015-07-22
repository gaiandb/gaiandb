/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

/**
 * Servlet implementation class GaianServlet
 */
@WebServlet("/GaianServlet")
@MultipartConfig
public class GaianServlet extends HttpServlet {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";
	
	private static final long serialVersionUID = 1L;

	private static GaianTask gaianTask;
	private static final String GAIAN_WORKSPACE_FOLDER_NAME = "GaianWorkspace";
	private String GAIAN_WORKSPACE_PATH; // = "/code/kepler1/GaianApp/WebContent/GaianWorkspace"; // or "dropins/GaianApp.war";
	
	@Override
	public void init() throws ServletException {

//		System.out.println("Java class path: " + System.getProperty("java.class.path"));
		
		ServletContext ctx = getServletContext();
		GAIAN_WORKSPACE_PATH = ctx.getRealPath( GAIAN_WORKSPACE_FOLDER_NAME );
		
		System.out.println("Servlet context: serverInfo: " + ctx.getServerInfo() + ", ctx path: " + ctx.getContextPath());
		System.out.println("GAIAN_WORKSPACE_PATH = ctx.getRealPath("+GAIAN_WORKSPACE_FOLDER_NAME+") = " + GAIAN_WORKSPACE_PATH);
		
		System.setProperty( "derby.system.home", GAIAN_WORKSPACE_PATH );
		gaianTask = new GaianTask( Arrays.asList("-initscript", GAIAN_WORKSPACE_PATH + "/initQueries.sql") );
		
		try { gaianTask.startTask(); }
		catch (Exception e) { System.out.println("Unable to start GaianTask, casue: " + e); e.printStackTrace(); }
	}
	
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		/* Set basic default properties for the response */
		response.setHeader("Cache-Control", "no-cache"); // HTTP 1.1
		response.setHeader("Pragma", "no-cache"); // HTTP 1.0
		response.setDateHeader("Expires", 0); // prevents caching at the proxy
		response.setContentType("text/json");
		
		PrintWriter writer = response.getWriter();
		writer.write("{}"); writer.flush(); writer.close(); // Prevents issue with client Dojo JSON parser: "unexpected end of input"
		
//		System.out.println( "Request Parts length: " + request.getParts().size() );
	    Part filePart = request.getPart("fileUploads[]"); // Gets file posted from: <input type="file" name="fileToUpload">
	    if ( null == filePart ) {
	    	System.err.println("Aborting due to missing request field part: 'fileUploads[]'");
	    	return;
	    }

		//System.out.println( "Got fileUploads[] part" );
	    
	    Part relativeDirPart = request.getPart("relativeDestinationFolder");
	    final String relativeDestinationFolder = null == relativeDirPart ? "" :
	    	new BufferedReader( new InputStreamReader( relativeDirPart.getInputStream() ) ).readLine() + "/";
	    
		//System.out.println( "Got relativeDestinationFolder: " + relativeDestinationFolder );
	    
	    final String fn = getFilename(filePart);

		//System.out.println( "Got file name: " + fn );
	    
	    if ( null != filePart )
//		    System.out.println("===> content-disposition header: " + filePart.getHeader("content-disposition"));
		    Util.copyBinaryData( filePart.getInputStream(),
		    		new FileOutputStream( new File(GAIAN_WORKSPACE_PATH + "/"
		    				+ (fn.endsWith(".jar")?"lib/"+fn:relativeDestinationFolder+fn) ) ) );
	}

	private static String getFilename(Part filePart) {
	    for (String cdElmt : Util.splitByTrimmedDelimiter(filePart.getHeader("content-disposition"), ';'))
	        if (cdElmt.startsWith("filename=\"")) {
	            String filename = cdElmt.substring("filename=\"".length(), cdElmt.length()-1); // also removes double quotes
	            return filename.substring(filename.lastIndexOf('/')+1).substring(filename.lastIndexOf('\\')+1); // I.E fix.
	        }
	    return null;
	}
	
	/**
	 * @see Servlet#destroy()
	 */
	public void destroy() {
		gaianTask.shutDown();
	}
}
