/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianNode;

import sun.misc.BASE64Encoder;

public class SecurityClientAgent {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";	

	private static final boolean IS_SECURITY_EXCLUDED_FROM_RELEASE = GaianNode.IS_SECURITY_EXCLUDED_FROM_RELEASE;
	
	public static final String GDB_CREDENTIALS = "GDB_CREDENTIALS";
		
	private static final String USR_SUFFIX = "_USR";
	private static final String PWD_SUFFIX = "_PWD";
	
	public static final String KEY_ALGORITHM_RSA = "RSA";
	public static final String CHECKSUM_ALGORITHM_MD5 = "MD5";
	public static final String CHECKSUM_ALGORITHM_SHA1 = "SHA1";
	public static final int ENCRYPTED_BLOCK_NUMBYTES_RSA = 64;

	private static KeyFactory keyFactory = null;
	private static Cipher cipher = null;

    // Code below is used to set and get the access credentials column which is a concatenation of all encrypted usr,pwd,checksum blocks.
    
	private Hashtable<String, byte[]> pubKeys = new Hashtable<String, byte[]>();
	private Hashtable<String, String> remoteAccessCredentials = new Hashtable<String, String>();
	
	public void setRemoteAccessCredentials( String nodeID, String usr, String pwd ) {
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return;
		
		remoteAccessCredentials.put( nodeID + USR_SUFFIX, usr );
		remoteAccessCredentials.put( nodeID + PWD_SUFFIX, pwd );
	}
	
	public void retainAllRemoteAccessCredentialsForNodes( Set<String> nodes ) {
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return;
		
		Set<String> keysToRetain = new HashSet<String>();
		for ( String node : nodes ) {
			keysToRetain.add( node + USR_SUFFIX );
			keysToRetain.add( node + PWD_SUFFIX );
		}
		
		Set<String> allKeysBackedByMap = remoteAccessCredentials.keySet();
		allKeysBackedByMap.retainAll(keysToRetain);
	}
	
	public void removeRemoteAccessCredentials( String nodeID ) {
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return;
		
		remoteAccessCredentials.remove( nodeID + USR_SUFFIX );
		remoteAccessCredentials.remove( nodeID + PWD_SUFFIX );
	}
	
//	public void clearRemoteAccessCredentials() {
//		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return;
//		remoteAccessCredentials.clear();
//	}
	
	public boolean isSecurityCredentialsSpecified() {
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return false;
		return !remoteAccessCredentials.isEmpty();
	}
	
	// Only use this if the calling code is running in the same JVM as the Derby network server.
	public void refreshPublicKeysFromServers() throws SQLException {
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return;
		refreshPublicKeysFromServers( GaianDBConfig.getEmbeddedDerbyConnection().createStatement() );
	}
	
	private static final int PUBLIC_KEYS_VALIDITY_PERIOD_MS = 10000; // 10 seconds
	private long lastPublicKeysRefreshTime = 0;
	
	public void refreshPublicKeysFromServers( Statement stmt ) throws SQLException {
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return;
		
		if ( lastPublicKeysRefreshTime + PUBLIC_KEYS_VALIDITY_PERIOD_MS > System.currentTimeMillis() )
			return;
		lastPublicKeysRefreshTime = System.currentTimeMillis(); // must be done before the sql call or we get an infinite loop
		
		String getPublicKeysSQL = "select gdb_node, pkey from "+
		"new com.ibm.db2j.GaianQuery('select gPublicKey() pkey from sysibm.sysdummy1','with_provenance') Q";
	
		ResultSet rs = stmt.executeQuery(getPublicKeysSQL);
		
		while ( rs.next() ) pubKeys.put( rs.getString(1), rs.getBytes(2) );
	}
    
    public String getEncryptedCredentialsValueInBase64( String sql ) throws Exception {
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return null;
    	
		byte[] queryChecksumBytes = getChecksum(sql.getBytes(), CHECKSUM_ALGORITHM_MD5);

		// 1 set of credentials taking NUMBYTES_RSA bytes for each recorded usr/pwd pair
		int credNumBytes = ENCRYPTED_BLOCK_NUMBYTES_RSA * remoteAccessCredentials.size()/2;
		
		byte[] credentials = new byte[ credNumBytes ];
		int i = 0;
		
		for ( String nodeID : pubKeys.keySet() ) {
			
			String usr = remoteAccessCredentials.get( nodeID + USR_SUFFIX );
			String pwd = remoteAccessCredentials.get( nodeID + PWD_SUFFIX );
			
			if ( null == usr || null == pwd ) continue; // no credentials for this server.
			
			byte[] decrypted = joinByteArrays(
					new byte[] { (byte) usr.length() }, usr.getBytes(),
					new byte[] { (byte) pwd.length() }, pwd.getBytes(),
					new byte[] { (byte) queryChecksumBytes.length }, queryChecksumBytes );
			
			byte[] encrypted = encrypt( decrypted, deriveRSAPublicKey(pubKeys.get(nodeID)) );
			
//			System.out.println("New credentials block before encryption: " + new String(decrypted));
//			System.out.println("New credentials block after encryption (length " + encrypted.length + "): " + new String(encrypted));
			
			if ( encrypted.length != ENCRYPTED_BLOCK_NUMBYTES_RSA )
				throw new Exception("Invalid length for encrypted credentials for " + nodeID + ": " + encrypted.length);
			
			System.arraycopy(encrypted, 0, credentials, i, encrypted.length);
			i += encrypted.length;
		}
		
		return new BASE64Encoder().encode(credentials).replaceAll("[\r\n]", ""); // remember to remove the inserted CRLFs
    }
    
	private static PublicKey deriveRSAPublicKey( byte[] publicKeyBytes ) throws NoSuchAlgorithmException, InvalidKeySpecException {
		if ( null == keyFactory ) keyFactory = KeyFactory.getInstance(KEY_ALGORITHM_RSA);
		return keyFactory.generatePublic( new X509EncodedKeySpec(publicKeyBytes) );
	}
	
	private static byte[] encrypt( byte[] decrypted, Key publicKey ) throws InvalidKeyException, IllegalBlockSizeException, 
									BadPaddingException, NoSuchAlgorithmException, NoSuchPaddingException {

		if ( null == cipher ) cipher = Cipher.getInstance(KEY_ALGORITHM_RSA);
		
		cipher.init(Cipher.ENCRYPT_MODE, publicKey);
		//System.out.println("Max encrypted byte[] length: " + cipher.getOutputSize( decrypted.length ) );
		byte[] encrypted = cipher.doFinal(decrypted);
		
		return encrypted;
	}
	
	private static byte[] getChecksum( byte[] input, String algo ) throws NoSuchAlgorithmException {
		
		MessageDigest checksum = MessageDigest.getInstance(algo);
		return checksum.digest(input);
	}
	
    private static byte[] joinByteArrays( byte[]... byteArrays ) {
    	
    	int resultSize = 0;
    	for ( byte[] b : byteArrays )
    		resultSize += b.length;
    	
    	byte[] result = new byte[ resultSize ];

    	int i=0;
    	for ( byte[] b : byteArrays ) {
    		System.arraycopy(b, 0, result, i, b.length);
    		i+=b.length;
    	}
    	
    	return result;
    }
}
