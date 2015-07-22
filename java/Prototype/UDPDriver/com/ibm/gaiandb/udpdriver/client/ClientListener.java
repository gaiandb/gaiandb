/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */


package com.ibm.gaiandb.udpdriver.client;



import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.udpdriver.common.SocketHelper;
import com.ibm.gaiandb.udpdriver.common.protocol.Message;
import com.ibm.gaiandb.udpdriver.common.protocol.MessageFactory;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * Listener on a SocketHelper.
 * The listened port is automatically provided by the system.
 * When receiving a new datagram, the listener behavior is :
 * 1 - Check if it is a STOP message, if yes it stops to listen and properly closes its SocketHelper.
 * 2 - Transform the datagram into a UDP driver protocol message
 * 3 - Place the new UDP driver protocol message in a concurrent structure so the application threads, 
 * running Statement or PreparedStatement objects, could reach it.
 * 
 * @author lengelle
 *
 */
public class ClientListener implements Runnable
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
    
	private static final Logger logger = new Logger( "ClientListener", 25 );
	
	/**
	 * If the ClientListener receives this message, it automatically stops its activity and closes its SocketHelper.
	 */
	public final static byte[] STOP_MESSAGE = { 'S', 'T', 'O', 'P' };
	
    private Thread thread;
    private SocketHelper clientSocket;
    private ConcurrentMap< String, BlockingQueue<Message> > map;

    /**
     * Creates a new ClientListener.
     * 
     * @param clientSocket the SocketHelper to listen on
     * @param map the structure shared by the application threads
     */
    public ClientListener( SocketHelper clientSocket, ConcurrentMap< String, BlockingQueue<Message> > map )
    {
        this.thread = new Thread( this,"UDP ClientListener" );
        this.clientSocket = clientSocket;
        this.map = map;
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
            byte[] inputData = null;
            
            while ( true )
            {
                try
                {
                    clientSocket.receive();
                    inputData = clientSocket.getByteArrayMessage();
                
                    if ( Arrays.equals( inputData, STOP_MESSAGE ) )
                    {
                    	break;
                    }
                    
                    message = MessageFactory.getMessage( inputData );
                    addToConcurrentMap( message );
                }
                catch( Exception e )
                {
                	logger.logException( GDBMessages.NETDRIVER_CLIENT_LISTENER_ERROR, "ClientListener run() failed.", e );
                }
            }
            
        }
        finally
        {
            clientSocket.close();
        }
    }
    
 
    /**
     * Add a message to the concurrent structure
     * 
     * @param message
     */
    private void addToConcurrentMap( Message message )
    {
        if ( map==null || message==null )
        {
            return;
        }
        
        String key = message.getQueryID();
        
        if( map.containsKey( key ) )
        {
            map.get( key ).offer( message );
        }
    }

    
//    private static boolean isStopMessage( byte[] message )
//    {
//        return Arrays.equals( message, STOP_MESSAGE ); //( message.length == STOP_MESSAGE.length );
//    }
}
