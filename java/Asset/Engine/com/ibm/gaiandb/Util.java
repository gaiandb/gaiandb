/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.ibm.db2.jcc.DB2Diagnosable;
import com.ibm.db2.jcc.DB2Sqlca;
import com.ibm.db2j.GaianTable;
import com.ibm.gaiandb.DataSourcesManager.RDBProvider;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * General utilities class
 * 
 * @author DavidVyvyan
 *
 */
public class Util {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "Util", 20 );
	
	static final List<String> EMPTY_LIST = new ArrayList<String>();
	static final Set<String> EMPTY_SET = new HashSet<String>();

	// Standard VARCHAR lengths
	public static final String TSTR = "VARCHAR(20)"; // tiny
	public static final String SSTR = "VARCHAR(80)"; // small
	public static final String MSTR = "VARCHAR(128)"; // medium
	public static final String LSTR = "VARCHAR(2000)"; // large
	public static final String VSTR = "VARCHAR(8000)"; // v. large
	public static final String XSTR = "VARCHAR(32672)"; // xtra large
	
	public static String[] splitByCommas( String list ) {
    	return splitByTrimmedDelimiter( list, ',' );
    }

	public static String[] splitByWhitespace( String list ) {
		if ( null == list || 0 == list.length() ) return new String[0];
		return list.trim().split("[\\s]+");
	}
	
	public static String[] splitByTrimmedDelimiter( String list, char delimiter ) {
		if ( null == list || 0 == list.length() ) return new String[0];
		// Note: a trailing empty value after the last comma will not appear in the resulting array
		return list.trim().split("[\\s]*" + delimiter + "[\\s]*");
	}
	
	/**
	 *  Split a string around instances of the delimiter (trimming white space too), but ignore any delmiters nested in quotes or within
	 *  bracketed argument lists, e.g: "a, b + 'g,k', myfunction('bla''h,h''ello',c,d)" -> "a", "b + 'g,k'", "myfunction('bla''h,h''ello',c,d)"
	 *  Also handle any depth of nesting... 
	 *  
	 * @param s String to be split
	 * @param delimiter delimiter to split it by
	 * @return
	 */
	public static String[] splitByTrimmedDelimiterNonNestedInCurvedBracketsOrSingleQuotes( String s, char delimiter ) {
		return splitByTrimmedDelimiterNonNestedInBracketsOrQuotesOrComments(s, delimiter, '\'', '(', true, -1, null, null);
	}
		
	public static String[] splitByTrimmedDelimiterNonNestedInCurvedBracketsOrDoubleQuotes( String s, char delimiter ) {
		return splitByTrimmedDelimiterNonNestedInBracketsOrQuotesOrComments(s, delimiter, '"', '(', true, -1, null, null);
	}
	
	public static String[] splitByTrimmedDelimiterNonNestedInCurvedBracketsOrQuotes( String s, char delimiter ) {
		return splitByTrimmedDelimiterNonNestedInBracketsOrQuotesOrComments(s, delimiter, '\0', '(', true, -1, null, null);
	}

	public static String[] splitByTrimmedDelimiterNonNestedInSquareBracketsOrDoubleQuotes( String s, char delimiter ) {
		return splitByTrimmedDelimiterNonNestedInBracketsOrQuotesOrComments(s, delimiter, '"', '[', true, -1, null, null);
	}
	
	// Splits by the delimiters that are not nested in '()' or both single or double quotes. (Implementation uses '\0' to test *both* quote chars)
	public static String[] splitByTrimmedDelimiterNonNestedInCurvedBracketsOrQuotes(
			String s, char delimiter, boolean includeEmptyElmts, int maxElmts, String cmtStart, String cmtEnd ) {
		return splitByTrimmedDelimiterNonNestedInBracketsOrQuotesOrComments(s, delimiter, '\0', '(', includeEmptyElmts, maxElmts, cmtStart, cmtEnd);
	}

	private static String[] splitByTrimmedDelimiterNonNestedInBracketsOrQuotesOrComments(
			final String s, final char delimiter, final char quoteChar, char bracketChar,
			final boolean includeIntermediaryEmptyElmts, final int maxElmts, String cmtStart, String cmtEnd ) {
		
		if ( null == s || 0 == s.length() || 0 == maxElmts ) return new String[0];
		
		char closingBracketChar;
		switch ( bracketChar ) {
			default: bracketChar='(';
			case '(': closingBracketChar = ')'; break; case '[': closingBracketChar = ']'; break;
			case '{': closingBracketChar = '}'; break; case '<': closingBracketChar = '>'; break;
		}
		
		ArrayList<String> results = new ArrayList<String>();

		int slen = s.length();
		int bracketNestingDepth = 0;
		int start = 0, elmtCount = 0;
		
		boolean isInQuotedExpression = false;
		int consecutiveQuoteCount = 0;
		char currentQuoteChar = quoteChar;

		if ( null == cmtStart || null == cmtEnd ) cmtStart = cmtEnd = null;
		final int cslen = null == cmtStart ? -1 : cmtStart.length(), celen = null == cmtEnd ? -1 :cmtEnd.length();
		boolean isInComment = false;
		
		for ( int i=0; i<slen; i++ ) {
			char c = s.charAt(i);
			
			if ( isInComment ) {
				// Check for end of a comment
				if ( i >= celen-1 ) {
					isInComment = false;
					for ( int k=0; k<celen; k++ )
						if ( cmtEnd.charAt(k) != s.charAt(i-celen+k+1) ) { isInComment = true; break; }
					if ( isInComment ) continue;
				}
			}
			
			// Not in a comment - look for nesting inside quotes..
			if ( '\0' == quoteChar && '\0' == currentQuoteChar && ('\''==c||'"'==c) ) currentQuoteChar = c;
			
//			if ( c == currentQuoteChar ) System.out.println("Hit quote at end of: " + (1<i?s.charAt(i-2):"") + (0<i?s.charAt(i-1):"") + c);
			
			if ( currentQuoteChar == c ) {
				// Only start counting consecutive quotes once we are IN the quoted expression
				if ( isInQuotedExpression ) consecutiveQuoteCount++;
				else isInQuotedExpression = true;
				continue;
			}
			
			// Test if we have been in a quoted expression
			if ( isInQuotedExpression ) {
				if ( 0 == consecutiveQuoteCount ) continue;
				// An uneven number of quotes marks the end of a nested quoted expression				
				isInQuotedExpression = 0 == consecutiveQuoteCount%2;
				consecutiveQuoteCount = 0;
				if ( isInQuotedExpression ) continue;
			}
			
			if ( '\0' == quoteChar ) currentQuoteChar = '\0';
			
			if ( bracketChar == c ) bracketNestingDepth++;
			else if ( closingBracketChar == c && 0 < bracketNestingDepth ) bracketNestingDepth--;
			
			if ( 0 < bracketNestingDepth ) continue;
			
			// Check if this is the start of a comment
			if ( null != cmtStart && i >= cslen-1 ) {
				isInComment = true;
				for ( int k=0; k<cslen; k++ )
					if ( cmtStart.charAt(k) != s.charAt(i-cslen+k+1) ) { isInComment = false; break; }
				if ( isInComment ) continue;
			}
			
			// Check if we hit a delimiter char
//			if ( c == delimiter ) System.out.println("Reached delimiter, with bnd: " + bracketNestingDepth + ", entry: " + s.substring(start, i).trim());
			if ( c == delimiter ) {
				final String entry = s.substring(start, i).trim();
				if ( includeIntermediaryEmptyElmts || 0 < entry.length() ) {
					results.add( entry );
					if ( 0 < maxElmts && ++elmtCount == maxElmts ) { slen = 0; break; }
				}
				start = i+1;
				continue;
			}
		}
		
		// Always add the last string - even if empty - (as the above code only deals with the strings having a delimiter at the end)
		if ( 0 < slen ) results.add( s.substring(start).trim() );
				
		return (String[]) results.toArray(new String[0]);
	}
	
	public static boolean isWhiteSpaceChar( char c ) {
		// In Pattern.java, whitespace is defined as one of: [ \t\n\x0B\f\r]
		// Note '\013' is the Octal value for unicode '\u000B' (i.e. \x0B), which is the vertical tab.
		switch (c) { case ' ': case '\t': case '\n': case '\013': case '\f': case '\r': return true; default: return false; }
	}
	
	public static String intArrayAsString(int[] a) {
		if ( null==a ) return null; int len = a.length;
		StringBuffer pcs = new StringBuffer( 0<len ? "[" + a[0] : "[" );
		for (int i=1; i<len; i++) pcs.append( ", " + a[i] ); pcs.append(']');
		return pcs.toString();
	}

	public static String longArrayAsString(long[] a) {
		if ( null==a ) return null; int len = a.length;
		StringBuffer pcs = new StringBuffer( 0<len ? "[" + a[0] : "[" );
		for (int i=1; i<len; i++) pcs.append( ", " + a[i] ); pcs.append(']');
		return pcs.toString();
	}
	
	public static String boolArrayAsString(boolean[] a) {
		if ( null==a ) return null; int len = a.length;
		StringBuffer pcs = new StringBuffer( 0<len ? "[" + a[0] : "[" );
		for (int i=1; i<len; i++) pcs.append( ", " + a[i] ); pcs.append(']');
		return pcs.toString();
	}

	public static String stringArrayAsCSV(String[] a) {
		return stringArrayAsCSV(a, null);
	}
	
	public static String stringArrayAsCSV(String[] a, RDBProvider rdbmsProvider) {
		if ( null==a ) return null; int len = a.length;
		StringBuffer pcs = new StringBuffer( 1>len ? "" :
				null == rdbmsProvider ? a[0] : GaianResultSetMetaData.wrapColumnNameForQueryingIfNotAnOrdinaryIdentifier(a[0], rdbmsProvider) );
		for (int i=1; i<len; i++) pcs.append( ", " +
				( null == rdbmsProvider ? a[i] : GaianResultSetMetaData.wrapColumnNameForQueryingIfNotAnOrdinaryIdentifier(a[i], rdbmsProvider) ) );
		return pcs.toString();
	}
	
	public static final void copyBinaryData( InputStream is, OutputStream os ) throws IOException {
		BufferedInputStream bis = new BufferedInputStream( is );
		BufferedOutputStream bos = new BufferedOutputStream( os );
		
		final int BUFFER_SIZE = 1<<10<<3; // 8K buffer
		byte[] buffer = new byte[BUFFER_SIZE];
		int numBytes = -1;
		while ((numBytes = bis.read(buffer)) > -1)
		      bos.write(buffer, 0, numBytes);
		
		bis.close(); is.close();
		bos.close(); os.close();
	}
	
	public static byte[] getFileBytes( File f ) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Util.copyBinaryData(new FileInputStream(f), baos);
		return baos.toByteArray();
	}
	
	public static String byteArray2HexString(byte[] b, boolean format) {
		if ( null == b ) return null;
		if ( 0 == b.length ) return "";
		String hex = Integer.toString( ( b[0] & 0xff ) + 0x100, 16).substring( 1 );
		for (int i=1; i < b.length; i++)
			hex += (format?"-":"") + Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
		return hex;
	}
	
	public static String getFileMD5( String path ) {
		return getFileMD5(new File(path));
	}

	public static String getFileMD5( File file ) {
		try { return byteArray2HexString( SecurityManager.getChecksumMD5( Util.getFileBytes( file ) ), false ).toUpperCase(); }
		catch ( Exception e ) { return "Unknown"; }
	}
	
	public static int intArrayContains(int[] a, int v) {
		for ( int i=0; i<a.length; i++ )
			if ( a[i] == v ) return i;
		return -1;
	}
	
	public static boolean stringsContainCommonChars( String a, String b ) {
		if ( null == a || null == b ) return false;
		for ( char c1 : a.toCharArray() )
			for ( char c2 : b.toCharArray() )
				if ( c1 == c2 ) return true;
		return false;
	}
	
	public static <T> Set<T> setDisjunction( Collection<T> a, Collection<T> b ) {
		Set<T> c = new HashSet<T>(a);
		c.addAll(b);
		for( Iterator<T> i = c.iterator(); i.hasNext(); ) {
			T o = i.next();
			if ( a.contains(o) && b.contains(o) ) i.remove();
		}
		return c;
	}
	
	public static String stripSingleQuotesDownOneNestingLevel( String s ) {
		return stripEscapeCharacterDownOneNestingLevel(s, '\'');
	}
	
	public static String stripBackSlashesDownOneNestingLevel( String s ) {
		return stripEscapeCharacterDownOneNestingLevel(s, '\\');
	}
	
	public static String stripEscapeCharacterDownOneNestingLevel( String s, char escapeChar ) {
		
    	if ( null == s ) return null;
    	
    	boolean isEscaped = false;
    	
    	StringBuffer sb = new StringBuffer();
    	for ( int i=0; i<s.length(); i++ ) {
    		char c = s.charAt(i);
    		if ( escapeChar == c ) {
    			isEscaped = !isEscaped;
    			if ( isEscaped ) continue;
    		} else
    			isEscaped = false;
    		sb.append(c);
    	}
    	
    	return sb.toString();
	}
	
	public static String escapeSingleQuotes( String s ) { return escapeCharactersByDoublingThem(s, '\''); }
	public static String escapeDoubleQuotes( String s ) { return escapeCharactersByDoublingThem(s, '"'); }
	
	public static String escapeCharactersByDoublingThem( String s, char escapeChar ) {
    	
		String escapeCharDoubled = escapeChar + "" + escapeChar;
    	if ( null == s ) return null;
    	String[] strs = s.split(escapeChar+"", -1); // use -1 to split even off a trailing quote
    	StringBuffer sb = new StringBuffer( strs[0] );
    	for (int i=1; i<strs.length; i++)
    		sb.append( escapeCharDoubled + strs[i] );
    	
    	return sb.toString();
    }
	
//	public static String escapeBackslashes( String s ) {
//    	
//    	if ( null == s ) return null;
//    	// 4 backslashes needed, 2 to escape the regex backslash, and 2 more to escape the java preprocessing.
//    	String[] strs = s.split("\\\\", -1); // use -1 to split even off a trailing quote
//    	StringBuffer sb = new StringBuffer( strs[0] );
//    	for (int i=1; i<strs.length; i++)
//    		sb.append( "\\\\" + strs[i] ); // 4 backslashes to insert 2
//    	
//    	return sb.toString();
//    }
	
	public static String escapeBackslashes( final String s ) {
		if ( null == s ) return null;
		StringBuilder sb = new StringBuilder(s);
		for ( int i = 0; i < sb.length(); i++ )
			if ( '\\' == sb.charAt(i) ) sb.insert(i++, '\\');
		return sb.toString();
	}
	
	public static String getExceptionAsString( Throwable e ) {
		
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);				
		e.printStackTrace(pw);
		pw.close();
		
		return "\n\t"+sw.toString();
	}
	
	public static String getAllExceptionCauses( Throwable e ) {
		Set<String> errors = new HashSet<String>();
		StringBuffer sb = new StringBuffer();
		Throwable cause = e;
		while (true) {
			cause = cause.getCause();
			if (null != cause) {
				if (!errors.contains(cause.getMessage())) {
					errors.add(cause.getMessage());
					sb.append(cause.getMessage() + "\n");
				}
			}
			else {
				break;
			}
		}
		return sb.toString();
	}
	
	public static String getStringWithLongestMatchingPrefix( String a, Set<String> candidates ) {
		
		String match = null;
		int currentml = 0;
		
		for ( String b : candidates ) {
			int ml = getMatchingStringLength(a, b);
			if ( currentml < ml ) {
				currentml = ml;
				match = b;
			}
		}
		
		return match;
	}
	
	public static int getMatchingStringLength( String a, String b ) {
		if ( null == a || null == b ) return 0;
		int alen = a.length(), blen = b.length();
		for ( int i=0; i<alen; i++ ) {
			if ( blen == i ) return blen;
			if ( a.charAt(i) != b.charAt(i) ) return i;
		}
		return alen;
	}
	
	public static Set<InetAddress> getAllMyHostAdresses() throws Exception {
		Set<InetAddress> addresses = new HashSet<InetAddress>();

		Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
		while ( en.hasMoreElements() ) {
			Enumeration<InetAddress> ias = en.nextElement().getInetAddresses();
			while( ias.hasMoreElements() )
				addresses.add( ias.nextElement() );
		}
		
		return addresses;
	}
	
	public static Set<InetAddress> getAllMyHostIPV4Adresses() throws Exception {
		Set<InetAddress> addresses = new HashSet<InetAddress>();

		Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
		while ( en.hasMoreElements() ) {
			Enumeration<InetAddress> ias = en.nextElement().getInetAddresses();
			while( ias.hasMoreElements() ) {
				InetAddress address = ias.nextElement();
				if ( isIPv4( stripToSlash( address.toString() ) ) ) addresses.add( address );
			}
		}
		
		return addresses;
	}
			
	public static boolean isIPv4(String ip) { return ip.matches("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+"); }
			
	public static String stripToSlash( String s ) { int idx = s.indexOf('/'); return -1 == idx ? s : s.substring( idx+1 ); }
	
	public static class NetInfo {
		
		private final String hostname;
		private final List<String> interfaceIDs = new ArrayList<String>(), interfaceInfos = new ArrayList<String>(),
			ipv4s = new ArrayList<String>(), broadcasts = new ArrayList<String>();
		private final List<Integer> netPrefixLengths = new ArrayList<Integer>();
		
		public NetInfo() throws Exception {
			hostname = InetAddress.getLocalHost().getHostName();
			
			Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
			
			while ( en.hasMoreElements() ) {
				NetworkInterface ni = en.nextElement();
				
				if (GaianNode.isJavaVersion6OrMore) {
					for ( InterfaceAddress ifa : ni.getInterfaceAddresses() ) {
						String ip = stripToSlash( ifa.getAddress().toString() );
						if ( isIPv4(ip) ) {
							
							Integer npl = new Integer( ifa.getNetworkPrefixLength() );
							InetAddress bcastAddress = ifa.getBroadcast();
							String bcast = null == bcastAddress ? null : stripToSlash( bcastAddress.toString() );
							
//							// Do some checking -> corrected: don't remove broadcast IPs whose prefix doesn't match the IP prefix - these can still be valid.
//							if ( null != bcast && 8 <= npl && false == ip.substring(0, ip.indexOf('.')).equals( bcast.substring(0,  bcast.indexOf('.')) ) ) {
//								logger.logInfo("Detected invalid broadcast entry in Java net info (doesn't match ip prefix - setting broadcast to null). IP: " +  ip
//										+ ", Broadcast: " + bcast + ", netPrefixLength: " + npl);
//								bcast = null;
//							}

							interfaceIDs.add( ni.getName() );			// repeat interface props for different IPs on it.
							interfaceInfos.add( ni.getDisplayName() );	// repeat interface props for different IPs on it.

							ipv4s.add(ip);
							broadcasts.add( bcast );
							netPrefixLengths.add( npl );
							
//							System.out.println("New ni ip: " + ni.getName() + " -> " + ip + " -> " + bcast + "->" + ifa.getNetworkPrefixLength());
						}
					}
				} else {
					Enumeration<InetAddress> ias = ni.getInetAddresses();
					while( ias.hasMoreElements() ) {
						String ip = stripToSlash( ias.nextElement().toString() );
						if ( isIPv4(ip) ) {
							
							interfaceIDs.add( ni.getName() );			// repeat interface props for different IPs on it.
							interfaceInfos.add( ni.getDisplayName() );	// repeat interface props for different IPs on it.
							
							ipv4s.add(ip);
							broadcasts.add( null );
							netPrefixLengths.add( null );
						}
					}
				}
			}
			
//			System.out.println("Built NetInfo() with numelmts: " + ipv4s.size());
		}
		
		List<String> getAllBroadcastIPs() {
			if ( GaianNode.isJavaVersion6OrMore ) return broadcasts;
			ArrayList<String> bcastOptions = new ArrayList<String>();
			for ( String ip : ipv4s ) bcastOptions.addAll( Util.deduceAllPossibleBroadcastIPs(ip) );
			return bcastOptions;
		}
		
		public Object[] getInfoForClosestMatchingIP( String ip ) throws Exception {
			int i = 0;
			String address = Util.getStringWithLongestMatchingPrefix( 
					null==ip || 0==ip.length() ? GaianNodeSeeker.getDefaultLocalIP() : ip,
					new HashSet<String>( ipv4s ) );
			for ( int k=0; k<ipv4s.size(); k++ )
				if ( ipv4s.get(k).equals(address) )
					{ i = k; break; }
			return new Object[] { hostname, interfaceIDs.get(i), interfaceInfos.get(i), 
					ipv4s.get(i), broadcasts.get(i), netPrefixLengths.get(i) };
		}
		
//		public String getAllInfoAsValuesString() {
//			if ( 1 > ipv4s.size() ) return null;
//			StringBuffer sb = new StringBuffer();
//			for ( int i=0; i<ipv4s.size(); i++ )
//				sb.append("('"+hostname+"','"+interfaceIDs.get(i)+"','"+interfaceInfos.get(i)+"','"+ipv4s.get(i)+"','"+broadcasts.get(i)+"',"+netPrefixLengths.get(i)+"),");
//			sb.deleteCharAt(sb.length()-1);
//			return sb.toString();
//		}
		
		public List<String> getAllInterfaceInfoAsListOfRowsWithAliasedColumnsForIPsPrefixed( String ipPrefix ) {
			if ( 1 > ipv4s.size() ) return null;
			List<String> rows = new ArrayList<String>();
			for ( int i=0; i<ipv4s.size(); i++ )
				if ( null == ipPrefix || ipv4s.get(i).startsWith(ipPrefix) ) {
					String bcastXpr = broadcasts.get(i);
					bcastXpr = null == bcastXpr ? "CAST(NULL AS " + TSTR + ")" : "'" + bcastXpr + "'";
					rows.add("'"+hostname+"' hostname,'"+interfaceIDs.get(i)+"' interface,'"+interfaceInfos.get(i)+"' description,'"
								+ipv4s.get(i)+"' ipv4,"+bcastXpr+" broadcast,CAST("+netPrefixLengths.get(i)+" as INT) NetPrefixLength");
				}
			return rows;
		}
	}
	
	public static List<String> getAllCurrentBroadcastIPs() throws Exception {
		return new NetInfo().getAllBroadcastIPs();
	}
	
	public static ArrayList<String> deduceAllPossibleBroadcastIPs( String ip ) {
		
		ArrayList<String> bips = new ArrayList<String>();
		
		String[] ipBytes = ip.split("\\.");
		
		if ( 4 > ipBytes.length ) return null;

		for ( int i=3; i>0; i-- ) {
			for ( String s : getAllRightMaskingCombinations(padString(dec2bin(Integer.parseInt(ipBytes[i])), '0', 8)) ) {
				ipBytes[i] = bin2dec(s) + "";
				bips.add(ipBytes[0] + "." + ipBytes[1] + "." + ipBytes[2] + "." + ipBytes[3]);
			}
			ipBytes[i] = "255";
		}
		
		return bips;
	}
	
	private static List<String> getAllRightMaskingCombinations( String bin ) {
		
		ArrayList<String> cbs = new ArrayList<String>();
		StringBuffer sb = new StringBuffer(bin);
		
		while( true ) {
			int idx = sb.lastIndexOf("0");
			if ( -1 == idx ) break;
			sb.replace(idx, idx+1, "1");
			cbs.add(sb.toString());
		}
		
		return cbs;
	}
	
	private static String padString( String s, char c, int toSize ) {

		int padCount = toSize - s.length();
		if ( 1 > padCount ) return s;
		StringBuffer sb = new StringBuffer();
		while ( padCount-- > 0 ) sb.append('0');
		
		return sb.toString() + s;
	}
	
	private static int bin2dec( String bin ) {		
		int dec=0, n = 1;
		for ( int i=bin.length()-1; i>=0; i-- ) {
			dec += n * ( '1' == bin.charAt(i) ? 1 : 0 );
			n <<= 1;
		}
		return dec;		
	}

	private static String dec2bin( int dec ) {
		if ( 0 == dec ) return "0";
		StringBuffer sb = new StringBuffer();
		while ( dec != 0 ) {
			sb.append( 1==dec%2 ? 1 : 0 );
			dec >>= 1;
		}
		return sb.reverse().toString();
	}
	
	/**
	 * This method tells us if a JDBC connection appears to be open and therefore capable of servicing queries.
	 * However it does not detect a connection to be invalid if a query against it hangs - unlike the DatabaseConnectionChecker - because it doesn't have the
	 * capability to determine that it is not a legitimate long-running query.
	 * 
	 * A hang can occur for example when a Host OS of a connected node dies.
	 * Subsequent TCP socket receive() methods can then fail to detect the error condition - hence why it waits indefinitely thereafter.
	 *  
	 * For this scenario, the faulty hanging Connection is only rooted out (and its connection pool cleared) if a data-source query runs against it.
	 * This is done is via GaianResult: When the data-source query takes too long to respond, it invokes DatabaseConnectionsChecker.rootOutHangingDataSources().
	 * This runs a very simple SQL "values 1" statement, and waits for a response within a configurable time defined using 
	 * property GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS, which should be tailored to the physical network latency.
	 * 
	 * At the moment the checker is tailored/limited to check connections held by data-source wrappers, and then abort data-source wrapper queries as a result.
	 * However in future one may consider re-factoring the checker so it can be called from here just for checking simple connections. This would allow us to clear
	 * connection pools sooner and remove some purging calls for connection pools in DataSourcesManager.
	 * 
	 * @param c The Connection to be checked.
	 * @return true is the Connection appears to be valid. false if connection.isClosed() is true after attempting to run a simple query.
	 */
	public static final boolean isJDBCConnectionValid( final Connection c ) {
		
		final AtomicInteger isValid = new AtomicInteger( -1 ); // -1 = not checked yet; 0 = closed; 1 = valid (i.e. working)
		
		// Check connection in a separate thread in case the query hangs
		new Thread("Connection validator for JDBC Connection object: " + c) {
			public void run() {
				
				// Test-mode simulated hang behaviour - ignore unless running tests.
				if ( GaianNode.isInTestMode() && GaianDBConfigProcedures.internalDiags.containsKey("hang_on_maintenance_poll") ) {

					GaianDBConfigProcedures.internalDiags.remove("hang_on_maintenance_poll"); // disable this straight away
					logger.logInfo("Simulating 10s hang on maintenance poll against JDBC connection using Thread.sleep(10000)");
					try { Thread.sleep(10000); } catch (InterruptedException e) {}
					logger.logInfo("Simulated hang on maintenance poll completed");
				}
				
				Statement stmt = null;
				try { stmt = c.createStatement(); stmt.execute("values 'dummy sql to check jdbc connection'"); }
				catch (Exception e) {} finally { try { if ( null != stmt ) stmt.close(); } catch ( Exception e1 ) {} }
				// Note there is no standard dummy statement hence the one above, but we'll still know better if the connection is closed after running it...
				try { isValid.set( c.isClosed() ? 0 : 1 ); } catch (Exception e) {}
				synchronized( isValid ) { isValid.notify(); }
			}
		}.start();
		
		synchronized ( isValid ) {
			try { if ( -1 == isValid.get() ) isValid.wait( 200 ); }
			catch( InterruptedException e ) {}
			return 0 != isValid.get(); // Assume the connection is still valid if the statement does not return in 200ms (e.g. high latency networks).
		}
	}
	
	public static String getDerbyDatabaseProperty( Statement derbyStatementForQuerying, String propertyKey ) throws SQLException {
		ResultSet rs = derbyStatementForQuerying.executeQuery("VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('" + propertyKey + "')");
		try { return rs.next() ? rs.getString(1) : null; }
		finally { rs.close(); }
	}
	
	public static void executeCreateIfDerbyTableDoesNotExist( Statement stmt, String schema, String table, String createTableSQL ) throws SQLException {
		if ( false == Util.isExistsDerbyTable( stmt.getConnection(), schema, table ) )
			stmt.execute( createTableSQL );
	}
	
	public static boolean isExistsDerbyTable( Connection derbyConnection, String schema, String table ) throws SQLException {
		ResultSet rs = derbyConnection.getMetaData().getTables(null, schema, table, null);
		boolean isTableExists = rs.next();
		// Must close ResultSet - vital step - otherwise we can hit db locks
		// seen with: vti.TestICARESTSelfSameQueryJoinsCached.testDistributedSearchSelfSameJoinRestrictedFetchSizeAgainstOtherNode
		rs.close();
		return isTableExists;
	}
	
	public static String getStackTraceElementDigest( StackTraceElement se ) {
		
//		String className = stackTraceTranslate.getOldClassName( se.getClassName() );
		String className = se.getClassName();
		String shortClassName = className.substring( className.lastIndexOf('.')+1 );
//		return shortClassName + "." + stackTraceTranslate.getOldMethodSignatures( className, se.getMethodName() ) + 
		return shortClassName + "." + se.getMethodName() + ":" + se.getLineNumber();
	}

	// Methods to get Call Stack Trace information ...
	
	public static String getStackTraceDigest() { return getStackTraceDigest( -1, -1 ); }
	
	/**
	 * Get stack info, starting at a given depth (so depth=0 would return nothing) and extracting a given count of elements upwards from there.
	 * e.g: "getStackTraceDigest(4, 3)" would extract 3 stack trace lines from depth 4, missing out just 1 line from the top of the trace.
	 * 
	 * @param depth
	 * @param count
	 * @return
	 */
	public static String getStackTraceDigest( int depth, int count ) {
		// Don't use Thread.currentThread().getStackTrace() as it extracts unhelpful deeper "getStackTrace() -> getStackTraceImpl()" method calls
		return getStackTraceDigest(new Exception(), depth, count);
	}
	
	public static String getStackTraceDigest( Throwable e ) { return getStackTraceDigest( e, -1, -1, true ); }
	
	public static String getStackTraceDigest( Throwable e, int depth, int count, boolean printExMessage ) {
		return 0 == depth ? "" : (printExMessage ? e+"\n\t=> TRACE:     " : "") + getStackTraceDigest(e, depth, count);
	}
	
	// depth to start working back from (i.e. 0 would return nothing), count is the number of elements to extract upwards from that position.
	public static String getStackTraceDigest( Throwable e, final int depthIn, final int countIn ) {
		
		int depth = depthIn, count = countIn;
		StackTraceElement[] ses = e.getStackTrace();
		
		if ( 0 == depth ) return "";
		StringBuffer chain = new StringBuffer();
		if ( depth < 1 || depth > ses.length ) depth = ses.length;
				
		for (int i=depth-1; i>0 && 0!=count; i--, count--)
			chain.append( (i<depth-1?" -> ":"") + getStackTraceElementDigest( ses[i] ) );
		
		if ( 0 != count )
			chain.append( (1<depth?" -> ":"") + getStackTraceElementDigest( ses[0] ) );
		
		Throwable eCause = e.getCause();
		if ( null != eCause && -1 == depthIn && -1 == countIn )
			chain.append( "\n\t=> CAUSED BY: " + getStackTraceDigest( eCause, depthIn, countIn ) );
		
		return chain.toString();
	}
	
	public static String getGaiandbInvocationTargetException( Throwable e ) {
		Throwable cause = e;
		while ( null != ( cause = cause.getCause() ) ) {
			String msg = cause.getMessage();
			if ( null != msg && -1 < msg.indexOf( GaianTable.IEX_PREFIX ) )
				return msg;
		}
		return null;
	}

	// isWindowsOS if the os.name sys property is not null and starts with "WINDOWS" in any case
	public static final boolean isWindowsOS = (System.getProperty("os.name")+"").toUpperCase().startsWith("WINDOWS");

	private static final String UNRESOLVED_PATH = "<UNRESOLVED_PATH>".intern();
	private static String BYTECODE_PATH = UNRESOLVED_PATH;
	private static String INSTALL_PATH = UNRESOLVED_PATH;
	
	static String getBytecodeLocation() {
		
//		Alternative methods to get install path...
//		String[] eps = new String[10];
//		try { eps[0] = System.getProperty("user.dir"); } catch (Exception e) { eps[0] = e.toString(); } // This is the execution dir - may not always be the install dir!
//		try { eps[1] = System.getenv("GDBH"); } catch (Exception e) { eps[0] = e.toString(); } // This may not be the install dir if it set in a customer script (which runs launchGaianServer independently)
//		try { eps[2] = Util.class.getProtectionDomain().getCodeSource().getLocation().toString(); } catch (Exception e) { eps[0] = e.toString(); } // gets URL rather than URI path
//		try { eps[3] = ClassLoader.getSystemClassLoader().getResource(GaianNode.class.getName().replace('.', '/')).getPath(); } catch (Exception e) { eps[0] = e.toString(); } // sometimes throws exception...
//		try { eps[4] = Util.class.getResource("GAIANDB.jar").getPath(); } catch (Exception e) { eps[0] = e.toString(); } // sometimes throws exception...
//		try { eps[5] = Util.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath(); } catch (Exception e) { eps[0] = e.toString(); } // this is the one we use
//		int i=0; for ( String ep : eps ) logger.logAlways("eps[" + (i++) + "] = " + ep);
		
		try {
			return !(UNRESOLVED_PATH.equals(BYTECODE_PATH)) ? BYTECODE_PATH :
				( BYTECODE_PATH = Util.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath() );
		} catch (Throwable e) { return UNRESOLVED_PATH; } // should never happen
	}
	
//	static String getBytecodeLocation() {
//		try {
//			if ( UNRESOLVED_PATH == BYTECODE_PATH ) {
//				String bytecodePath = Util.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
//				BYTECODE_PATH = bytecodePath.endsWith(".jar") ? bytecodePath.substring( 0, bytecodePath.lastIndexOf( '/' ) ) : bytecodePath;
//			}
//				
//		} catch (URISyntaxException e) { return BYTECODE_PATH = null; } // should never happen
//		
//		return BYTECODE_PATH;
//	}
	
	public static String getInstallPath() {
		
		if ( UNRESOLVED_PATH.equals(INSTALL_PATH) ) {
			try {
				String bytecodePath = getBytecodeLocation();
				
//				System.out.println("installPath: " + installPath);
				String libDir = -1 == bytecodePath.lastIndexOf("/lib/") ? "/build/" : "/lib/";
				
				// Strip the jar file name and a preceding "/lib/" substring, or everything past the "/bin" substring (when running from eclipse)
				int idx = bytecodePath.lastIndexOf( bytecodePath.endsWith(".jar") ? libDir : "/bin" );
				
				if ( -1 != idx ) { 
					INSTALL_PATH = bytecodePath.substring(0, idx);

					// Class path comes with special characters, but they ruin file search
//					path = path.replaceAll("%20"," ");
			
					if (isWindowsOS) while ('/' == INSTALL_PATH.charAt(0)) INSTALL_PATH = INSTALL_PATH.substring(1);
				}
				else throw new Exception("Cannot find '/lib/' or '/build/' or '/bin' in bytecode path: " + bytecodePath);
			}
			catch (Exception e) {
				logger.logWarning(GDBMessages.NODE_UNRESOLVED_INSTALL_PATH, "GaianDB was unable to resolve install path: " + e);
				INSTALL_PATH = null;
			}
		}
		
		return INSTALL_PATH;
	}
	
	public static void moveFileAndLogOutcome(File fromFile, File toFile) throws Exception {

		final String fromFilePath = fromFile.getPath(), toFileName = toFile.getName();
		
		toFile.setWritable(true);
		
		// First, try to just rename the file
		if ( false == fromFile.renameTo(toFile) ) {
			logger.logInfo("Unable to rename file: " + fromFilePath + " to " + toFileName + "; attempting to delete target first...");

			String errMessage = null;
			
			// Try deleting the file and renaming after
			if ( false == toFile.delete() )
				errMessage = "Unable to delete file: " + toFileName + "; attempting to copy to it directly...";
			else if ( false == fromFile.renameTo(toFile) ) // fails (on windows) if 'fromFile' still has open file descriptors
				errMessage = "Unable to rename file (after target deleted) from " + fromFilePath + " to " + toFileName + "; attempting to copy it directly...";
			else
				logger.logInfo("Successfully renamed file (after target deleted) from " + fromFilePath + " to " + toFileName);
			
			if ( null != errMessage ) {
				logger.logInfo(errMessage);
				try { Util.copyFile(fromFile, toFile); }
				catch (IOException ioe) { throw new Exception("Unable to copy file " + fromFilePath + " to " + toFileName); }
				logger.logInfo("Successfully copied file " + fromFilePath + " to " + toFileName);
				
				// Only delete tmp config file after a successful copy (no need after a rename!)
				if ( false == fromFile.delete() )
					logger.logInfo("Unable to delete lingering temp file (after successful copy): " + fromFilePath);
			}

		} else
			logger.logInfo("Successfully renamed file (without target deletion) from " + fromFilePath + " to " + toFileName);	
	}
	
	public static void moveFile(File fromFile, File toFile) throws Exception {

//		final String fromFilePath = fromFile.getPath(), toFilePath = toFile.getPath(), toFileName = toFile.getName();
//		
//		toFile.setWritable(true);
//		
//		// Try to rename the file directly
//		if ( fromFile.renameTo(toFile) ) return;
//		
//		// Try to delete 'toFile' first and try a rename again after. This may fail if 'fromFile' has open file descriptors...
//		// First back-up 'toFile'
//		File toFileBackup = new File(toFilePath + ".backup");
//		try { Util.copyFile(toFile, toFileBackup); }
//		catch (IOException ioe) { throw new Exception("Unable to backup/copy file " + toFilePath + " to " + toFilePath+".bak"); }
//		
//		if ( true == toFile.delete() && fromFile.renameTo(toFile) ) return;
//		
		final String fromFilePath = fromFile.getPath(), toFileName = toFile.getName();
		
		toFile.setWritable(true);
		
		// Try to rename the file directly; OR try to delete 'toFile' first and try a rename again after. This may fail if 'fromFile' has open file descriptors...
		if ( fromFile.renameTo(toFile) || ( true == toFile.delete() && fromFile.renameTo(toFile) ) ) return;
		
		// Last resort: Do a hard copy of the file.
		try { Util.copyFile(fromFile, toFile); }
		catch (IOException ioe) { throw new Exception("Unable to copy file " + fromFilePath + " to " + toFileName); }
		
		// Only delete tmp config file after a successful copy (no need after a rename!)
		fromFile.delete(); // may not succeed...
	}
	
	public static boolean copyFile(String fromFileName, String toFileName) throws IOException {
		return copyFile(new File(fromFileName), new File(toFileName));
	}
	
	public static boolean copyFile(File fromFile, File toFile) throws IOException {
	    
//		System.out.println("Testing if files exist. "
//				+ "fromFile: " + fromFile.getCanonicalPath() + ": " + fromFile.exists()
//				+ "; toFile: " + toFile.getCanonicalPath() + ": " + toFile.exists()
//		);
		
		if ( !fromFile.exists() ) return false;
		if ( !toFile.exists() ) toFile.createNewFile();
	    FileChannel fromChannel = null, toChannel = null;

	    try {
	    	fromChannel = new FileInputStream(fromFile).getChannel();
	    	toChannel = new FileOutputStream(toFile).getChannel();
	    	toChannel.transferFrom(fromChannel, 0, fromChannel.size());
	    	return true;
	    } finally {
	        if ( null != fromChannel ) fromChannel.close();
	        if ( null != toChannel ) toChannel.close();
	    }
	}
	
	public static String getDB2Msg(SQLException sqle, boolean complete )
	{
		try { Util.class.getClassLoader().loadClass(DB2Sqlca.class.getName()); }
		catch ( Throwable e ) { return null; } //DB2 class DB2Sqlca cannot be loaded"; }
		
		StringBuilder msg = new StringBuilder();
		
		while(sqle != null) {             // Check whether there are more
			// SQLExceptions to process
			//=====> Optional DB2-only error processing
			if (sqle instanceof DB2Diagnosable) {   // Check if DB2-only information exists
				com.ibm.db2.jcc.DB2Diagnosable diagnosable =(com.ibm.db2.jcc.DB2Diagnosable)sqle;
				//diagnosable.printTrace (printWriter, "");
				java.lang.Throwable throwable = diagnosable.getThrowable();
				if (throwable != null) {
					// Extract java.lang.Throwable information
					// such as message or stack trace.
					//TODO
				}
				DB2Sqlca sqlca = diagnosable.getSqlca();
				if (sqlca != null) {              // Check that DB2Sqlca is not null
					int sqlCode = sqlca.getSqlCode();       // Get the SQL error code
					String sqlErrmc = sqlca.getSqlErrmc();  // Get the entire SQLERRMC
					String[] sqlErrmcTokens = sqlca.getSqlErrmcTokens();
					// You can also retrieve the individual SQLERRMC tokens
					String sqlErrp = sqlca.getSqlErrp();      // Get the SQLERRP
					int[] sqlErrd = sqlca.getSqlErrd();       // Get SQLERRD fields
					char[] sqlWarn = sqlca.getSqlWarn();      // Get SQLWARN fields
					String sqlState = sqlca.getSqlState();    // Get SQLSTATE
					String errMessage=null;
					try{
						errMessage = sqlca.getMessage();   // Get error message
					}catch(Exception ee){ } ;

					if (complete)
					{
						msg.append ("Server error message: " + errMessage);
						msg.append ("--------------- SQLCA ---------------");
						msg.append ("Error code: " + sqlCode);
						msg.append ("SQLERRMC: " + sqlErrmc);
						for (int i=0; i< sqlErrmcTokens.length; i++) {
							msg.append ("  token " + i + ": " + sqlErrmcTokens[i]);
						}
						msg.append ( "SQLERRP: " + sqlErrp );
						msg.append ( "SQLERRD(1): " + sqlErrd[0] + "\n" +
								"SQLERRD(2): " + sqlErrd[1] + "\n" +
								"SQLERRD(3): " + sqlErrd[2] + "\n" +
								"SQLERRD(4): " + sqlErrd[3] + "\n" +
								"SQLERRD(5): " + sqlErrd[4] + "\n" +
								"SQLERRD(6): " + sqlErrd[5] );
						msg.append ( "SQLWARN1: " + sqlWarn[0] + "\n" +
								"SQLWARN2: " + sqlWarn[1] + "\n" +
								"SQLWARN3: " + sqlWarn[2] + "\n" +
								"SQLWARN4: " + sqlWarn[3] + "\n" +
								"SQLWARN5: " + sqlWarn[4] + "\n" +
								"SQLWARN6: " + sqlWarn[5] + "\n" +
								"SQLWARN7: " + sqlWarn[6] + "\n" +
								"SQLWARN8: " + sqlWarn[7] + "\n" +
								"SQLWARN9: " + sqlWarn[8] + "\n" +
								"SQLWARNA: " + sqlWarn[9] );
						msg.append ("SQLSTATE: " + sqlState);   // portion of SQLException
					}
					else
					{
						String SsqlCode=("0000"+(-sqlCode));
						msg.append("SQL"+ (SsqlCode.substring(SsqlCode.length()-4 ))+" "+errMessage );
					}
				}
				else { //not diagnosable, but still an exception
					if (sqle.getSQLState()!= null )
					msg.append("SQLSTATE "+ (sqle.getSQLState())+" "+sqle.getMessage() );
				}
			}
			sqle=sqle.getNextException();     // Retrieve next SQLException
		}
		
		return msg.toString();
	}
	
	
	public static int getStringEditDifference(String s1, String s2) {
		
		if ( null == s1 ) return null == s2 ? 0 : s2.length();
		else if ( null == s2 ) return null == s1 ? 0 : s1.length();
		if ( s1.length() > s2.length() ) { String t = s1; s1 = s2; s2 = t; } // we will create 2 arrays of size equal to the shortest string length
		
		int[] diff = new int[s1.length()+1], next = new int[s1.length()+1]; // create 2 flat arrays
		for ( int i=0; i<diff.length; i++ ) diff[i] = i; // initialise char differences (assumes chars will be different at each position)
		
		for ( int i=1; i<s2.length()+1; i++, next[0] = i-1 ) {
			for ( int j=1; j<diff.length; j++ )
				next[j] = Math.min( Math.min( next[j-1] + 1, diff[j] + 1 ),
        				diff[j-1] + (s2.charAt(i-1) == s1.charAt(j-1) ? 0 : 1) );
			int[] temp = diff; diff = next; next = temp;
		}

        return diff[diff.length-1];
	}
	
	public static int getStringSimilarityPercentage(String s1, String s2) {
		int l = getStringEditDifference(s1, s2);
		int len = Math.max( null == s1 ? 0 : s1.length(), null == s2 ? 0 : s2.length() );
		
		return l*100 / len;
	}
	
	public static boolean stringMatchesWilcardExpression( String text, String expr ) {

		if ( null == text || null == expr ) return false;
		
		String[] frags = expr.split("\\*");
		
		if ( 0 < expr.length() && '*' != expr.charAt(0) && false == text.startsWith(frags[0]) ) return false;
		if ( 0 < expr.length() && '*' != expr.charAt(expr.length()-1) && false == text.endsWith(frags[frags.length-1]) ) return false;
		
	    for ( String frag : frags ) {
//	    	System.out.println("Looking for fragment: '" + frag + "' in suffix string: " + text);
	        int idx = text.indexOf(frag);
	        if ( -1 == idx ) return false;
	        text = text.substring(idx + frag.length());
	    }
	    return true;
	}
	
	public static String trimConsecutivePathSeparatorsToSingleOnes( String s ) {
		
		final StringBuilder sb = new StringBuilder(s);
		
		char c;
		for ( int start = 1, idx = 0; idx < sb.length(); start = idx + 1 ) {
//			logger.logThreadDetail( "trimConsecutivePathSeparatorsToSingleOnes() remaining string: " + sb.substring(idx) );
			do c = sb.charAt(idx++); while ( isSeparatorChar(c) && idx < sb.length() );
			int delta = idx - 1 - start;
			if ( delta > 0 ) {
				sb.delete(start, start+delta);
				idx -= delta; // shift the index back by the number of deleted characters
			}
		}
		
		return sb.toString();
	}
	
	public static boolean isSeparatorChar( char c ) { return '/' == c || isWindowsOS && '\\' == c; } // Note '/' is considered a file separator on all platforms.
	
	public static boolean isAbsolutePath( final String path ) {
		int pathLen = path.length();
		// NOTE: Do not assume that a Windows path will have a '\' path separator - '/' can also be valid.
		return Util.isWindowsOS ? 2 < pathLen && ':' == path.charAt(1) && isSeparatorChar(path.charAt(2)) 
								: 0 < pathLen && '/' == path.charAt(0);
	}
	
	/**
	 * Resolves all file paths matching a given maskedPath.
	 * The full path is split using the patform separator char ('/' on unix, '\' on windows), and each path element expression is resolved into existing folder or file names.
	 * 
	 * If 'isMatchByRegex' is false, then only the '*' character will have special meaning as a wildcard matching any character sequence for a given path element.
	 * 
	 * If 'isMatchByRegex' is true, then each element of the maskedPath (split by the separator char) is treated as a regular expression.
	 * Therefore when 'isMatchByRegex' is true, the '.' and '..' expressions will no longer interpreted as 'current folder' and 'parent folder',
	 * but rather 'any character' and 'any 2 characters in sequence'.
	 * 
	 * @param maskedPath
	 * @param isMatchByRegex
	 * @return The full set of file paths matching the maskedPath, either by simple wildcard matching or by regular expression.
	 */
	public static String[] findFilesTreeMatchingMask( String maskedPath, final boolean isMatchByRegex ) {
		
//		StringBuilder sb = new StringBuilder( regexPath );
//		
//		for ( int i = 0; i < sb.length() ; i++ ) {
//			switch ( sb.charAt(i) ) {
//			case '.': sb.insert(i++, '\\'); continue;
//			case '*': sb.insert(i++, '.'); continue;
//			}
//		}
//		
//		regexPath = sb.toString();
		
		maskedPath = trimConsecutivePathSeparatorsToSingleOnes( maskedPath );
		logger.logThreadInfo("Getting matching files for maskedPath: " + maskedPath);
		boolean isAbsolutePath = isAbsolutePath(maskedPath);
		
		String parentPrefix;
		
		if ( isAbsolutePath ) {
			parentPrefix = maskedPath.substring(0, Util.isWindowsOS ? 3 : 1);
			maskedPath = maskedPath.substring(Util.isWindowsOS ? 3 : 1);
		} else {
			parentPrefix = System.getProperty("user.dir");
			if ( false == isSeparatorChar( parentPrefix.charAt( parentPrefix.length()-1 ) ) ) parentPrefix += File.separatorChar;
		}
		
		logger.logThreadInfo("Resolved isAbsolutePath? " + isAbsolutePath + ", parentPrefix: " + parentPrefix + ", maskedPath: " + maskedPath);
		
		List<String> matches = findFilesTreeMatchingMask( parentPrefix, maskedPath, new HashSet<String>(), isMatchByRegex );
		
		if ( isAbsolutePath )
			return matches.toArray( new String[0] );

		// Remove absolute folder locations as they were not in the original mask.
		String[] relativeMatches = new String[ matches.size() ];
		int prefixLen = parentPrefix.length();
		for ( int i=0; i<relativeMatches.length; i++ )
			relativeMatches[i] = matches.get(i).substring(prefixLen);
		return relativeMatches;
	}
	
	public static int indexOfFileSeparator( String s ) {
		// Note '/' is also valid on windows
		int idx1 = s.indexOf( '/' );
		if ( false == isWindowsOS ) return idx1;
		int idx2 = s.indexOf( '\\' );
		if ( -1 == idx1 ) return idx2;
		if ( -1 == idx2 ) return idx1;
		return Math.min(idx1, idx2);
	}
	
	private static List<String> findFilesTreeMatchingMask( final String parentPrefix, String maskedPath, final Set<String> visitedFolders, final boolean isMatchByRegex ) {
		
		final File parent = new File( parentPrefix ); // includes trailing separator char
		if ( false == parent.isDirectory() ) return EMPTY_LIST;
		
		final int separatorIndex = indexOfFileSeparator( maskedPath );
		final boolean isLeafElmt = -1 == separatorIndex;
		
		if ( isLeafElmt )
			try { if ( false == visitedFolders.add( parent.getCanonicalPath() ) ) return EMPTY_LIST; } // we've been here before
			catch (IOException e) { return EMPTY_LIST; } // ignore this folder
					
		// get the next path element, escaping the $ ^ ( ) characters that are valid in filenames but also 
	    // special characters in RegExes. If you don't mask these characters then the pattern matcher below
	    // treats then as special characters and they probably won't match. e.g. a $ in a filename will be
		// treated as a end of line match.
		final String nxtPathElmtMask = maskedPath.substring( 0, -1 == separatorIndex ? maskedPath.length() : separatorIndex ).replaceAll("([\\$\\^\\(\\)])", "\\\\$1");
		
		logger.logThreadDetail("Getting matching files for parent: " + parentPrefix + ", nxtPathElmtMask: " + nxtPathElmtMask);
		
		String[] flist;
		
		if ( false == isMatchByRegex && -1 == nxtPathElmtMask.indexOf('*') )
			flist = new String[] { nxtPathElmtMask }; // this is key to handling the '.' and '..' expressions for navigating to "current" and "parent" folders
		else {
			// Apply the corresponding regex as filter to all files at the folder location
			
			final Pattern fileNamePattern = isMatchByRegex ? Pattern.compile( nxtPathElmtMask ) : null;
			flist = parent.list( new FilenameFilter() {
				@Override public boolean accept(File dir, String name) {
					return isMatchByRegex ? fileNamePattern.matcher( name ).matches() : stringMatchesWilcardExpression(name, nxtPathElmtMask); }
			});
		}
		
		if ( null == flist ) return EMPTY_LIST; // not a folder or IOException - shouldn't happen

		logger.logThreadDetail("Derived subtree elements: " + Arrays.asList(flist));

		List<String> matchingFilePaths = new ArrayList<String>();
		
		if ( isLeafElmt )
			for ( String pathElmt : flist )
				matchingFilePaths.add( parentPrefix + pathElmt );
		else {
			maskedPath = maskedPath.substring( separatorIndex+1 ); // get ready to search one level deeper
			
			for ( String pathElmt : flist )
				matchingFilePaths.addAll( findFilesTreeMatchingMask( parentPrefix + pathElmt + File.separatorChar, maskedPath, visitedFolders, isMatchByRegex ) );
		}
			
		return matchingFilePaths;
	}
	
	public static int runSystemCommand( final String[] sysCommand, boolean isPrintLog ) throws IOException {
		
		if ( isPrintLog ) System.out.println("Running system command: " + Arrays.asList(sysCommand));
		
		Process process = (isPrintLog ? new ProcessBuilder().inheritIO() : new ProcessBuilder() ).command( sysCommand ).start();		
		try { return process.waitFor(); } catch (InterruptedException e) { e.printStackTrace(); }

		return -1;
	}
	
	public static int runSystemCommand( final String[] sysCommand, final Object synchObject, boolean isPrintLog ) throws IOException {
		
		if ( null == synchObject ) return runSystemCommand( sysCommand, isPrintLog );
		else synchronized ( synchObject ) { return runSystemCommand( sysCommand, isPrintLog ); }
	}
	
}
