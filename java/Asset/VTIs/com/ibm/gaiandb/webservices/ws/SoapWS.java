/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.ws;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
//import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPConstants;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;

/**
 * 
 * Access a SOAP web service.
 * 
 * @author remi - IBM Hursley
 *
 */
public class SoapWS extends WebService {

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

	/** The parameter to add to the url in order to get the WSDL. */
	private static final String REQ_WSDL = "?wsdl";
	
	
	// -------------------------------------------------------------------------- Dynamic
	
	/** The stream on which one the data is received. */ 
	private InputStream data;
	
	private InputStream wsdl;
	
	private String operation;
	
	private HashMap<String, String> attributes;

	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public

	/**
	 * Generates SoapWS object. same than SoapWS(Strng, String, String, but gives 
	 * the url "[url]?wsdl" for defining the wsdl.
	 * @param url
	 * @param formatedOperation
	 * 			String containing the particularities of the operation. <br/>
	 * Format: operation[, attributeName, value]
	 */
	public SoapWS(String url, String formatedOperation) throws MalformedURLException {
		this(url,url + REQ_WSDL, formatedOperation);
	}
	/**
	 * Generates SoapWS object.
	 * @param url
	 * @param urlWsdl
	 * 			wsdl location. must start with File:/// if local file, and by http:// if external
	 * @param formatedOperation
	 * 			<p>
	 * 			String containing the particularities of the operation.
	 * 			<p>
	 * 			The format is:													<br/>
	 * 			operationName[, parameterName, parameterValue]* 		
	 */
	public SoapWS(String url, String wsdlUrl, String formatedOperation) throws MalformedURLException {
		super(url);
		
		// Define operation and attributes
		String[] values = formatedOperation.split(",");
		
		this.operation = values[0];
		
		this.attributes = new HashMap<String, String>();
		for (int i = 1; i + 1 < values.length; i=i+2) {
			this.attributes.put(values[i], values[i+1]);
		}
		
		// wsdl
		RestWS ws = new RestWS(wsdlUrl);
		try {
			ws.openConnection();
			this.wsdl = ws.getInputStream();
		} catch (IOException e) {
			// Shouldn't happen
		}
		
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	@Override
	public void openConnection() throws IOException {
		// ---------- Get the web service's WSDL -------------------------
		WsdlSweeper sweeper = new WsdlSweeper(this.wsdl);
		String swNameSpace = sweeper.getNamespace();
		String swService = sweeper.getServiceName();
		String swPort = sweeper.getPortname();
		
		// ---------- Generate parameters to send to the server ----------
		QName serviceName = new QName(swNameSpace, swService);
		QName portName = new QName(swNameSpace, swPort);
		
		// ---------- Create a service and add at least one port to it ---------- 
		Service service = Service.create(this.receiver, serviceName);
//		service.addPort(portName, SOAPBinding.SOAP11HTTP_BINDING, this.receiver.toString());
//		service.addPort(portName, SOAPBinding.SOAP11HTTP_BINDING, endpointUrl);
				
		// ---------- Create a Dispatch instance from a service ----------
		Dispatch<SOAPMessage> dispatch = service.createDispatch(portName, SOAPMessage.class, Service.Mode.MESSAGE);
			
		try {
			// ---------- Create SOAPMessage request ----------
			// composes a request message
			MessageFactory mf = MessageFactory.newInstance(SOAPConstants.SOAP_1_1_PROTOCOL);
	
			// Creates a message. 
			SOAPMessage request = mf.createMessage();
			SOAPPart part = request.getSOAPPart();
	
			// Obtains the SOAPEnvelope and body elements.
			SOAPEnvelope env = part.getEnvelope();
			SOAPBody body = env.getBody();
	
			// Constructs the message payload.
			SOAPElement operation = body.addChildElement(this.operation);
			Set<String> setAttributes = this.attributes.keySet();
			for (String attribute : setAttributes) {
				SOAPElement value = operation.addChildElement(attribute);
				value.addTextNode(this.attributes.get(attribute));
			}
			
			request.saveChanges();
	
			// ---------- Invokes the service endpoint ----------
			SOAPMessage response = dispatch.invoke(request);
			
			// ---------- Processes the answer ----------
			this.data = new ByteArrayInputStream(
					response.getSOAPBody().getTextContent().getBytes());
		} catch (SOAPException e) {
			// TODO - manage exception
			e.printStackTrace();
		}
	}
	
	@Override
	public InputStream getInputStream() throws IOException {
		// ---------- Present the result ---------------------------------
		return this.data;
	}

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	
}
