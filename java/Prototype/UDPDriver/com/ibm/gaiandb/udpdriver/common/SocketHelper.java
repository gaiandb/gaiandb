/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.common;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Acts as an interface between the other UDP driver classes and the java.net package.
 * It offers useful methods which aim to help manipulating the UDP sockets.
 * SocketHelper internally uses a java.net.DatagramSocket.
 * 
 * @author lengelle
 *
 */
public class SocketHelper
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	/**
	 * The size in bytes of the buffer in which are placed incoming datagrams.
	 * Note: if an incoming datagram packet is bigger than DATAGRAM_BUFFER_SIZE, the datagram
	 * will be truncated.
	 */
	private static int DATAGRAM_BUFFER_SIZE = 64000;
	
	/**
	 * Set the buffer size
	 * 
	 * @param newSize
	 */
	public static void setBufferSize( int newSize )
	{
		DATAGRAM_BUFFER_SIZE = newSize;
	}
	
    private DatagramSocket socket;
    private DatagramPacket incomingPacket;
    private byte[] buffer;
    
    /**
	 * Constructs a datagram socket and binds it to any available port on the local host 
	 * machine. The socket will be bound to the wildcard address, an IP address chosen 
	 * by the kernel. 
     * The buffer size default value is 64000 bytes.
     * 
     * @see java.net.DatagramSocket
     * @throws IOException
     */
    public SocketHelper() throws IOException
    {
        buffer = new byte[ DATAGRAM_BUFFER_SIZE ];
        incomingPacket = new DatagramPacket( buffer, buffer.length );
        
        socket = new DatagramSocket();
    }
    
    
    /**
	 * Creates a datagram socket, bound to the specified local address. The local port 
	 * must be between 0 and 65535 inclusive. If the IP address is 0.0.0.0, the socket 
	 * will be bound to the wildcard address, an IP address chosen by the kernel. 
     * The buffer size default value is 64000 bytes.
     * 
     * @see java.net.DatagramSocket
     * @param serverAdress
     * @param serverPort
     * @throws IOException
     */
    public SocketHelper( String serverAdress, int serverPort ) throws IOException
    {
        buffer = new byte[ DATAGRAM_BUFFER_SIZE ];
        incomingPacket = new DatagramPacket( buffer, buffer.length );
        
        socket = new DatagramSocket( serverPort, InetAddress.getByName( serverAdress ) );
    }
    
    /**
     * Sends the message given as a parameter to the address and port specified.
     * This automatically creates a new java.net.DatagramPacket object.
     * 
     * @see java.net.DatagramPacket
     * @param message
     * @param address
     * @param port
     * @throws IOException
     */
    public void send( byte[] message, InetAddress address, int port ) throws IOException
    {
        DatagramPacket packet = new DatagramPacket( message, message.length, address, port );
        socket.send( packet );
    }
    
    /**
     * Returns the data of the last packet received on this socket as a byte array.
     * 
     * @return
     */
    public byte[] getByteArrayMessage()
    {
        if ( incomingPacket == null )
        {
            return null;
        }
    	
    	return Util.getSubArray( incomingPacket.getData(), 0, incomingPacket.getLength() );
    }
    
    /**
     * Returns the address bound to the last datagram received.
     * 
     * @return
     */
    public InetAddress getPacketAddress()
    {
        if ( incomingPacket == null )
        {
            return null;
        }
        return incomingPacket.getAddress();
    }
    
    
    /**
     * Returns the port bound to the last datagram received.
     * 
     * @return
     */
    public int getPacketPort()
    {
        if ( incomingPacket == null )
        {
            return -1;
        }
        return incomingPacket.getPort();
    }
    
    /**
     * Equivalent to java.net.DatagramSocket.receive method.
     * 
     * @throws IOException
     */
    public void receive() throws IOException
    {
        socket.receive( incomingPacket );
    }
    
    /**
     * Equivalent to java.net.DatagramSocket.getInetAddress method.
     * 
     * @return
     */
    public InetAddress getLocalAddress()
    {
    	return socket.getInetAddress();
    }
    
    
    /**
     * Equivalent to java.net.DatagramSocket.getLocalPort method.
     * 
     * @return
     */
    public int getLocalPort()
    {
    	return socket.getLocalPort();
    }
    
    
    /**
     * Equivalent to java.net.DatagramSocket.close method.
     * 
     * @return
     */
    public void close()
    {
        socket.close();
    }
}
