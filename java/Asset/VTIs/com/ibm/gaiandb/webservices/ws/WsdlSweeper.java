/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.ws;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

//import com.ibm.CORBA.iiop.Request;

/**
 * The purpose of this class is to get important info from a wsdl.
 * This info contains the namespace, the portname and the service name 
 * 
 * @author remi - IBM Hursley
 *
 */
public class WsdlSweeper {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";

	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/** Definition of the namespace contained in the wsdl. */
	private String namespace;
	
	/** Name of the service defined in the wsdl. */
	private String portname;
	
	/** Port the service uses, defined in the wsdl. */
	private String serviceName;
	

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	public WsdlSweeper(InputStream wsdlContent) {
		
	// This constructor will create a Dom tree and find the different 
	// information a soap web service needs to send a Request.
	// 
	// We could have not created 3 different attributes and request
	// each of them thanks to a xpath request, but it would have meant 
	// that the Dom tree would have stayed in the memory much longer
	// (since it would have had to be an attribute to the object).
	// 
	// With this method, the Dom tree is only built the time of the 
	// constructor, and is then removed.
		
		// ------------ Generate the dom tree from the wsdl file ------------	
		DocumentBuilderFactory domBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder domBuilder = null;
		Document wsdl = null;
		try {
			domBuilder = domBuilderFactory.newDocumentBuilder();
		    wsdl = domBuilder.parse(wsdlContent);
		} catch (ParserConfigurationException e) {
		    e.printStackTrace(); 
		} catch (SAXException e) {
		    e.printStackTrace();
		} catch (IOException e) {
		    e.printStackTrace();
		}
		
		// ------------ Generate the xpath object ------------	
		XPath xPath =  XPathFactory.newInstance().newXPath();
		String nameSpaceExpr = "/definitions/@targetNamespace";
		String serviceExpr = "/definitions/service/@name";
		String portExpr = "/definitions/service/port/@name";
		
//		System.out.println(nameSpaceExpr);
//		System.out.println(serviceExpr);
//		System.out.println(portExpr);
		 
		// ------------ Defines values to find in wsdl ------------	
		try {
			this.namespace = xPath.compile(nameSpaceExpr).evaluate(wsdl);
		} catch (XPathExpressionException e) {
			this.namespace = null;
		}
		try {
			this.serviceName = xPath.compile(serviceExpr).evaluate(wsdl);
		} catch (XPathExpressionException e) {
			this.serviceName = null;
		}
		try {
			this.portname = xPath.compile(portExpr).evaluate(wsdl);
		} catch (XPathExpressionException e) {
			this.portname = null;
		}
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Returns the wsdl's namespace.
	 * @return the wsdl's namespace. 
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * Returns the wsdl's portname.
	 * @return the wsdl's portname. 
	 */
	public String getPortname() {
		return portname;
	}

	/**
	 * Returns the wsdl's service name.
	 * @return the wsdl's service name.
	 */
	public String getServiceName() {
		return serviceName;
	}
	

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
