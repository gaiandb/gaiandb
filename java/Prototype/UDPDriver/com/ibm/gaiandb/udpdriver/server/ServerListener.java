/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.server;



import java.net.InetAddress;
import java.util.concurrent.Executor;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.udpdriver.common.SocketHelper;
import com.ibm.gaiandb.udpdriver.common.protocol.Message;
import com.ibm.gaiandb.udpdriver.common.protocol.MessageFactory;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * Listener on a SocketHelper.
 * When receiving a new datagram, the listener behavior consists of :
 * 1 - Transform the datagram into a UDP driver protocol message
 * 2 - Give the newly created protocol message to a thread (from the thread pool)
 * 
 * @author lengelle
 *
 */
public class ServerListener implements Runnable
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final Logger logger = new Logger( "ServerListener", 25 );
	
    private Thread thread;
    private SocketHelper socket;
    private Executor executor;
    private ConnectionManager connectionManagerForPreparedStatement;
    private ConnectionManager connectionManagerForStatement;
    
    
    public ServerListener( SocketHelper socket, Executor executor, ConnectionManager connectionManagerForPreparedStatement, ConnectionManager connectionManagerForStatement )
    {
        this.thread = new Thread( this,"UDP ServerListener" );
        this.thread.setName( "UDPDriver-Server-Listener" );
        
        this.socket = socket;
        this.executor = executor;
        this.connectionManagerForPreparedStatement = connectionManagerForPreparedStatement;
        this.connectionManagerForStatement = connectionManagerForStatement;
    }
    
    /**
     * Starts listening
     */
    public void start()
    {
        thread.start();
    }

    /**
     * This method should not be used.
     * It is preferable to use start()
     */
    public void run()
    {
        try
        {
                
            Message message = null;
            byte[] data = null;
            InetAddress clientAddress = null;
            int clientPort = 0;
            
            while( true )
            {
            	try
            	{
                    socket.receive();
                    
                    data = socket.getByteArrayMessage();
                    clientAddress = socket.getPacketAddress();
                    clientPort = socket.getPacketPort();
                                        
//                    System.out.println("Received packet from address " + clientAddress + ", port " + clientPort + ", message: " + new String(data));
                    
                    message = MessageFactory.getMessage( data, clientAddress, clientPort );
                    
                    executor.execute( new RunnableWorker( message, socket, connectionManagerForPreparedStatement, connectionManagerForStatement ) );
                    
            	}
            	catch( Exception e )
            	{
            		logger.logException( GDBMessages.NETDRIVER_RUN_ERROR, "ServerListener run() failed.", e );
            	}
            }
        }
        finally
        {
        	socket.close();
        }
    }

}
