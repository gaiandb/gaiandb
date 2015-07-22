/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import sun.misc.BASE64Decoder;

import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.tools.SQLDerbyRunner;

public class SecurityManager {
	
	private static final boolean IS_SECURITY_EXCLUDED_FROM_RELEASE = GaianNode.IS_SECURITY_EXCLUDED_FROM_RELEASE;
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	private static final Logger logger = new Logger( "SecurityManager", 25 );
	
	public static final String CREDENTIALS_LABEL = "GDB_CREDENTIALS";
	
	public static final int ENCRYPTED_BLOCK_NUMBYTES_RSA = 64;
	
	public static final String KEY_ALGORITHM_RSA = "RSA";
	private static final int KEY_NUMBITS_RSA = 512;
	
	public static final String CHECKSUM_ALGORITHM_MD5 = "MD5"; // Confirms a signature - hash takes 128 bits
	public static final String CHECKSUM_ALGORITHM_SHA1 = "SHA1"; // Much harder to derive hash collisions, but takes 160 bits
	
	private static KeyPair keyPair = null;
	
	//private static KeyFactory keyFactory = null;
	private static Cipher cipher = null;
	
	/**
	 * Method used to retrieve the public key for this server. The key is typically sent to clients for encryption of their data.
	 * 
	 * @return A serialized public key 
	 * @throws SQLException
	 */
	public static byte[] getPublicKey() throws SQLException {
		
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return null;
		
		if ( null == keyPair || null == cipher )
			try{ initKeysAndCipher( KEY_ALGORITHM_RSA, KEY_NUMBITS_RSA ); }
			catch (Exception e) { throw new SQLException("Internal Error: " + e); }
    	
    	return keyPair.getPublic().getEncoded();
	}
	
//	public static void initKeysAndCipher(String keyAlgorithm) throws NoSuchAlgorithmException, NoSuchPaddingException {
//		initKeysAndCipher(keyAlgorithm, KEY_NUMBITS_RSA);
//	}
	
    private static void initKeysAndCipher(String keyAlgorithm, int numBits) throws NoSuchAlgorithmException, NoSuchPaddingException {
		// Get the public/private key pair and a Cipher instance for encrypting/decrypting
		KeyPairGenerator keyGen = KeyPairGenerator.getInstance(keyAlgorithm);
		keyGen.initialize(numBits);
		keyPair = keyGen.genKeyPair();
		cipher = Cipher.getInstance(keyAlgorithm);
	}
    
//    /**
//     * Encrypt the given byte[] with the local static public key - this would not normally be used on the server side.
//     * 
//     * @param decrypted
//     * @param publicKey
//     * @return
//     * @throws InvalidKeyException
//     * @throws IllegalBlockSizeException
//     * @throws BadPaddingException
//     * @throws NoSuchAlgorithmException
//     * @throws NoSuchPaddingException
//     */
//	public static byte[] encrypt( byte[] decrypted, Key publicKey ) throws InvalidKeyException, IllegalBlockSizeException, 
//							BadPaddingException, NoSuchAlgorithmException, NoSuchPaddingException {
//		
//		if ( null == cipher ) cipher = Cipher.getInstance(KEY_ALGORITHM_RSA);
//		
//		cipher.init(Cipher.ENCRYPT_MODE, publicKey);
////		System.out.println("Max encrypted byte[] length: " + cipher.getOutputSize( decrypted.length ) );
//		byte[] encrypted = cipher.doFinal(decrypted);
//		
//		return encrypted;
//	}
	
	/**
	 * Use the local static generated private key to decrypt a message that was presumably encrypted by the associated public key.
	 * If the message cannot be decrypted, the method will either throw an Exception or return garbage.
	 * 
	 * @param encrypted
	 * @return
	 * @throws InvalidKeyException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchPaddingException
	 */
	private static byte[] decryptUsingUniqueLocalPrivateKey( byte[] encrypted ) throws Exception {
		
		if ( null == keyPair || null == cipher )
			throw new Exception("Failed to decrypt bytes: Keys have not been generated (getPublicKey() was never called)");
		
		cipher.init(Cipher.DECRYPT_MODE, keyPair.getPrivate());
		byte[] decrypted = cipher.doFinal(encrypted);
		
		return decrypted;
	}
	
//	/**
//	 * De-serializes a public key
//	 * 
//	 * @param publicKeyBytes
//	 * @return
//	 * @throws NoSuchAlgorithmException
//	 * @throws InvalidKeySpecException
//	 */
//	public static PublicKey deriveRSAPublicKey( byte[] publicKeyBytes )
//							throws NoSuchAlgorithmException, InvalidKeySpecException {
//		if ( null == keyFactory ) keyFactory = KeyFactory.getInstance(KEY_ALGORITHM_RSA);
//		return keyFactory.generatePublic( new X509EncodedKeySpec(publicKeyBytes) );
//	}
//
//	/**
//	 * De-serializes a private key - should not normally be used, unless the private key has been passed around securely itself.
//	 * 
//	 * @param privateKeyBytes
//	 * @return
//	 * @throws NoSuchAlgorithmException
//	 * @throws InvalidKeySpecException
//	 */
//	public static PrivateKey deriveRSAPrivateKey( byte[] privateKeyBytes )
//							throws NoSuchAlgorithmException, InvalidKeySpecException {
//		if ( null == keyFactory ) keyFactory = KeyFactory.getInstance(KEY_ALGORITHM_RSA);
//		return keyFactory.generatePrivate( new PKCS8EncodedKeySpec(privateKeyBytes) );
//	}
	
	/**
	 * Returns a checksum for the given byte[], given a checksum algorithm such as CHECKSUM_ALGORITHM_MD5
	 * 
	 * @param input
	 * @param algo
	 * @return
	 * @throws NoSuchAlgorithmException
	 */
	private static byte[] getChecksum( byte[] input, String algo ) throws NoSuchAlgorithmException {		
		MessageDigest checksum = MessageDigest.getInstance(algo);
		return checksum.digest(input);
	}
	
	public static byte[] getChecksumSHA1( byte[] input ) throws NoSuchAlgorithmException {
		return getChecksum( input, CHECKSUM_ALGORITHM_SHA1 );
	}
	
	public static byte[] getChecksumMD5( byte[] input ) throws NoSuchAlgorithmException {
		return getChecksum( input, CHECKSUM_ALGORITHM_MD5 );
	}
	
	public static String[] verifyCredentials( String b64EncodedMultiEncryptedBlock/*, String sqlQueryIn*/ ) throws SQLException {
		
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return null;
		
		String authenticatedUser = null;
		
		// We have a credentials column value that was passed in - we need to check the query hash and authenticate the user.
		try {
			byte[] multiEncryptedColumn = new BASE64Decoder().decodeBuffer( b64EncodedMultiEncryptedBlock );
//			System.out.println("multiencrypted value len: " + multiEncryptedColumn.length + ", EACH BLOCK SIZE: " + SecurityManager.ENCRYPTED_BLOCK_NUMBYTES_RSA);
			for ( int i=0; i<=multiEncryptedColumn.length-ENCRYPTED_BLOCK_NUMBYTES_RSA; i+=ENCRYPTED_BLOCK_NUMBYTES_RSA ) {
				byte[] credEncrypted = new byte[ENCRYPTED_BLOCK_NUMBYTES_RSA];
				System.arraycopy(multiEncryptedColumn, i, credEncrypted, 0, ENCRYPTED_BLOCK_NUMBYTES_RSA);
				
				byte[] credentials = null;
				
				try { credentials = decryptUsingUniqueLocalPrivateKey(credEncrypted); }
				catch (Exception e) { logger.logInfo("Credentials segment from block not recognised for this node: " + e); continue; }
				
				byte[][] fields = new byte[3][];
				int idx = 0, pos = 0; // each field is preceded by its size which occupies 1 byte
				for ( byte[] f : fields ) {
					int fsize = credentials[pos++];
					f = new byte[fsize];
					System.arraycopy(credentials, pos, f, 0, fsize);
					fields[idx++] = f;
					pos += fsize;
				}
				String usr = new String(fields[0]), pwd = new String(fields[1]);
				
				// Don't bother with the 3rd field for now (checksum)
//				byte[] originalChecksum = fields[2];
//				logger.logInfo("SQL query in: " + sqlQueryIn);
//				logger.logInfo("Checksums match: " + Arrays.equals( originalChecksum, getChecksum(sqlQueryIn.getBytes(), CHECKSUM_ALGORITHM_MD5)));
				
//				if ( Arrays.equals( originalChecksum, getChecksum(sqlQueryIn.getBytes(), CHECKSUM_ALGORITHM_MD5) ) &&
//						authenticateUser(usr, getChecksum(pwd.getBytes(), CHECKSUM_ALGORITHM_SHA1)) ) {

				if ( authenticateUser( usr, getChecksumSHA1(pwd.getBytes()) ) ) {
					authenticatedUser = usr;
					logger.logInfo("Successfully authenticated user " + authenticatedUser);
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			String errmsg = "Could not extract user credentials from DataValueDescriptor: " + e;
			logger.logThreadWarning(GDBMessages.ENGINE_CREDENTIALS_VERIFY_ERROR, "DERBY ERROR: " + errmsg);
			throw new SQLException( errmsg );
		}

		if ( null == authenticatedUser )
			logger.logInfo("Unable to authenticate a user from the credentials block");
		
		return getUserFields(authenticatedUser);
	}
	
	private static final String GDB_USERS_TABLE = "GDB_USERS";
	private static final String colUser = "gdbuser";
	private static final String colAffiliation = "affiliation";
	private static final String colClearance = "clearance";
	private static final String colPassword = "password";
	
	private static Connection dedicatedConnection = null;
	private static PreparedStatement pstmtGetPwd = null;
	private static PreparedStatement pstmtSetPwd = null;
	private static PreparedStatement pstmtRegisterUser = null;
	private static PreparedStatement pstmtRemoveUser = null;
	private static PreparedStatement pstmtGetUserFields = null;
	
	private static void establishConnection() throws SQLException {
		if ( null == dedicatedConnection || dedicatedConnection.isClosed() ) {
			// Get connection to admin data
			// Best to use embedded driver connection... otherwise, would need to consider whether sslMode is set..
			dedicatedConnection = GaianDBConfig.getEmbeddedDerbyConnection();
			pstmtGetPwd = null;
			pstmtSetPwd = null;
			pstmtRegisterUser = null;
			pstmtRemoveUser = null;
		}
	}
	
	public static void initialiseUsersTableAndItsUpdateTrigger(SQLDerbyRunner sdr) throws Exception {
		
		if ( IS_SECURITY_EXCLUDED_FROM_RELEASE ) return;
		
		dedicatedConnection = sdr.getConnection();
		establishConnection();
		Statement stmt = dedicatedConnection.createStatement();
		try {
			if ( false == Util.isExistsDerbyTable( dedicatedConnection, null, GDB_USERS_TABLE ) ) {
				
				stmt.execute("create table " + GDB_USERS_TABLE + "(" + colUser + " " + Util.TSTR + 
					", " + colAffiliation + " " + Util.TSTR + ", " + colClearance + " " + Util.TSTR + 
					", " + colPassword + " CHAR(20) FOR BIT DATA, PRIMARY KEY (" + colUser + "))");
			}
		} catch ( SQLException e ) {
			logger.logWarning(GDBMessages.ENGINE_USER_TABLE_INIT_ERROR, "Could not create GDB_USERS table: " + e);
		};
//		try { stmt.execute("drop procedure RegisterUser"); } catch ( SQLException e ) {}
//		stmt.execute("create procedure RegisterUser (" + colUser + " " + Util.TSTR + ", " + colPassword + " " + Util.TSTR + ")" +
//				" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.registerUser'");
		
		final String gdbtrigger = "gdbtrigger";
		final String gdbTriggerSQL = ""
		+ "!DROP FUNCTION "+gdbtrigger+" ;!CREATE FUNCTION "+gdbtrigger+"(S "+Util.XSTR+") RETURNS INT"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setTriggerEvent'"
		+ ";"
		+ "!DROP TRIGGER GDB_USERS_UPDATED;"
		+ " !CREATE TRIGGER GDB_USERS_UPDATED AFTER UPDATE ON GDB_USERS FOR EACH STATEMENT MODE DB2SQL"
		+ " select 1 from new com.ibm.db2j.GaianQuery('select "+gdbtrigger+"(''GDB_USERS_UPDATED'') from sysibm.sysdummy1') GQ"
		+ ";"
		;
		sdr.processSQLs( gdbTriggerSQL );
	}
	
	static void registerUser( String usr, String affiliation, String clearance, String pwd ) throws SQLException, NoSuchAlgorithmException {
		establishConnection();
		if ( null == pstmtRegisterUser )
			pstmtRegisterUser = dedicatedConnection.prepareStatement("insert into " + GDB_USERS_TABLE + " values(?, ?, ?, ?)");
		pstmtRegisterUser.setString(1, usr);
		pstmtRegisterUser.setString(2, affiliation);
		pstmtRegisterUser.setString(3, clearance);
		pstmtRegisterUser.setBytes(4, getChecksumSHA1(pwd.getBytes()));
		pstmtRegisterUser.execute();
	}
	
	static void removeUser( String usr ) throws SQLException {
		establishConnection();
		if ( null == pstmtRemoveUser )
			pstmtRemoveUser = dedicatedConnection.prepareStatement("delete from " + GDB_USERS_TABLE + " where " + colUser + "=?");
		pstmtRemoveUser.setString(1, usr);
		pstmtRemoveUser.execute();
	}
	
	private static String[] getUserFields(String usr) throws SQLException {
		establishConnection();
		if ( null == pstmtGetUserFields )
			pstmtGetUserFields = dedicatedConnection.prepareStatement(
					"select " + colAffiliation + ", " + colClearance + " from " + GDB_USERS_TABLE + " where " + colUser + "=?");
		pstmtGetUserFields.setString(1, usr);
		ResultSet rs = pstmtGetUserFields.executeQuery();
		
		if ( !rs.next() ) {
			logger.logWarning(GDBMessages.ENGINE_USER_FIELDS_GET_ERROR, "Unable to extract user fields - no entry found for user: " + usr);
			return null;
		}
		String[] userFields = new String[3];
		userFields[0] = usr;
		userFields[1] = rs.getString(1);
		userFields[2] = rs.getString(2);
		
		if ( rs.next() ) {
			logger.logWarning(GDBMessages.ENGINE_USER_CREDENTIALS_ERROR, "Error case detected: more than one credentials entry was found for user: " + usr);
			return null;
		}
		
		return userFields;
	}
	
	private static boolean authenticateUser( String usr, byte[] pwdHashToAuthenticate ) throws SQLException, NoSuchAlgorithmException {
		
		establishConnection();
		if ( null == pstmtGetPwd ) pstmtGetPwd = dedicatedConnection.prepareStatement(
				"select " + colPassword + " from " + GDB_USERS_TABLE + " where " + colUser + "=?");
		if ( null == pstmtSetPwd ) pstmtSetPwd = dedicatedConnection.prepareStatement(
				"update " + GDB_USERS_TABLE + " set " + colPassword + "=? where " + colUser + "=?");		
		
		pstmtGetPwd.setString(1, usr);
		pstmtGetPwd.execute();
		ResultSet rs = pstmtGetPwd.getResultSet(); //executeQuery();
		
		if ( !rs.next() ) {
			logger.logWarning(GDBMessages.ENGINE_USER_NOT_FOUND, "User not found on local server: " + usr);
			rs.close();
			return false;
		}
		
		byte[] pwdHash = rs.getBytes(1);
		rs.close();
		
		if ( null == pwdHash || 0 == pwdHash.length ) {
			logger.logWarning(GDBMessages.ENGINE_USER_PASSWORD_BLANK, "Setting password (currently blank) from incoming query for user " + usr);
			pstmtSetPwd.setBytes(1, pwdHashToAuthenticate);
			pstmtSetPwd.setString(2, usr);
			pstmtSetPwd.execute();
			
		} else if ( !Arrays.equals( pwdHash, pwdHashToAuthenticate ) ) {
			logger.logWarning(GDBMessages.ENGINE_USER_PASSWORD_INCORRECT, "Incorrect password entered for user: " + usr);
			return false;
		}
		
		return true;
	}
}
