/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.db2j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ibm.gaiandb.GaianChildRSWrapper;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * Abstract class for providing durable caching to child VTIs. <br />
 * <br />
 * Durable caches live longer than the current instance of the query and allow
 * cached results to be shared between multiple invocations of the same query.
 * These caches expire after a set amount of time (defined by cache.expires for
 * the VTI). <br />
 * The child VTI should call cleanUpCaches() from it's close() method to ensure
 * that the appropriate cache housekeeping is regularly performed. <br />
 * <br />
 * This class provides utility methods to ensure there are no issues as a result
 * of concurrent modification of the cache, reading from an incomplete cache or
 * inappropriate deletion of active caches. <br />
 * The lifecycle of a child VTI should be: <br />
 * 1. Call the appropriate super(...) method from the constructor.<br />
 * 2. If required, call setExtension(...) to define the 'unique id' of this
 * query.<br />
 * ...<br />
 * 3. Call markCacheInUse(true) to mark the cache as logically in use (so it is
 * not deleted prematurely).<br />
 * ...<br />
 * 4. Call getCacheModifyLock() to retrieve the cache modification lock.<br />
 * 5. Obtain the lock and perform any data retrieval and cache population steps.
 * If caching is enabled, only the first invocation of your VTI for a particular
 * query should need to do any actual fetching and population of the cache. Use
 * isCached(...) to determine if results are already cached.<br />
 * 6. Release the lock (<b>IMPORTANT:</b> You should perform this in a finally
 * block (try{...}finally{...}) to ensure it is released even in the case of
 * Exception).<br />
 * ...<br />
 * 7. Call markCacheInUse(false) to mark the cache as no longer logically in
 * use.<br />
 * ...<br />
 * 8. Call cleanUpCaches() from the close() method.<br />
 * <br />
 * Note: if setExtension() is to be called, it should be before any other method
 * of this class is called.
 * 
 * @author stephen.nicholas
 */
public abstract class AbstractDurableCacheVTI extends AbstractVTI {

	// Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	private static final Logger logger = new Logger("AbstractDurableCacheVTI", 40);

	// The 'current' cache table suffix - increments each time a new cache table is used
	private static AtomicLong cacheTableSuffix = new AtomicLong(0);

	// Map of query_ids to locks - these locks are used to ensure no concurrent modification of the cache
	private static Map<String, Lock> cacheModifyLockMap = new ConcurrentHashMap<String, Lock>();

	// Map of query_ids to cache use count (the number of people logically still using the cache)
	private static Map<String, AtomicLong> cacheUseCountMap = new ConcurrentHashMap<String, AtomicLong>();

	// The cache table name for this VTI - instantiated in getCacheTableName()
	private String cacheTableName = null;

	public AbstractDurableCacheVTI() throws Exception { this(null, null); }
	public AbstractDurableCacheVTI(String constructor) throws Exception { this(constructor, null); }

	public AbstractDurableCacheVTI(String constructor, String extension) throws Exception {
		super(constructor, extension);

		long expiryDuration = getExpiryDuration();
		if (0 >= expiryDuration) {
			logger.logImportant("Cache expiry duration is <= 0 - caching disabled");
			isCached = -1;
		}
	}

	@Override
	public boolean isCached(String constraints) {
		try {
			logger.logImportant("Checking caching state, isCached == " + isCached + ", rows constraint: " + constraints);

			if (isCached > -1) {

				Connection conn = getPooledLocalDerbyConnection();
				Statement stmt = conn.createStatement();

				try {
					boolean cacheTableAlreadyExists = findOrCreateCacheTableAndExpiryEntry(stmt);

					if (cacheTableAlreadyExists) {
						logger.logInfo("Cache table exists: " + getCacheSchemaAndTableName());

						// Don't "select *" as it may contain cache index
						// cols which we don't want to expose
						String sql = "SELECT " + getTableMetaData().getColumnNames() + " FROM " + getCacheSchemaAndTableName()
								+ (null != constraints && 0 < constraints.trim().length() ? " WHERE " + constraints : "");

						logger.logInfo("Executing query: " + sql);
						underlyingResultSet = conn.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ).executeQuery(sql);

						if (underlyingResultSet.next()) {
							logger.logImportant("Found cached rows for constraints: " + constraints);
							isCached = 1; // Rows are cached
							underlyingResultSet.beforeFirst();
							resultRows = new GaianChildRSWrapper(underlyingResultSet);
						} else {
							logger.logImportant("No cached rows for contraints: " + constraints);
							underlyingResultSet.close();
							underlyingResultSet = null;
						}

					} else {
						logger.logImportant("Initialised cache table: " + getCacheSchemaAndTableName());
					}
				} finally {
					if (null == underlyingResultSet) {
						// No rows cached yet - no need to keep stmt open
						stmt.close();
						recyclePooledLocalDerbyConnection(conn);
					}
				}
			}
		} catch (Exception e) {
			isCached = -1;
			logger.logException( GDBMessages.DSWRAPPER_DURABLE_CACHE_TABLES_INIT_ERROR, "Unable to initialise cache tables (caching disabled)", e);
		}

		logger.logImportant("isCached() status " + isCached + " for table " + getCacheSchemaAndTableName() + ", returning " + (isCached == 1));
		return isCached == 1;
	}

	@Override
	public void setExtension(String extension) {
		super.setExtension(extension);
		getCacheTableName();
	}

	@Override
	protected String getCacheSchemaAndTableName() {
		return "CACHE." + getCacheTableName();
	}

	@Override
	protected String getCacheTableName() {

		// If the cache table name has not been initialised & caching is enabled
		if (cacheTableName == null && isCached != -1) {

			// See if a suitable cache already exists
			try {
				Connection conn = null;
				Statement stmt = null;

				try {
					conn = getPooledLocalDerbyConnection();
					stmt = conn.createStatement();
					
					if ( true == Util.isExistsDerbyTable(conn, "CACHE", "EXPIRES") ) {
						
						ResultSet rs = stmt.executeQuery("SELECT name, lastreset FROM CACHE.EXPIRES WHERE name LIKE 'CACHE." + super.getCacheTableName() + "_%" + "'");
	
						while (rs.next()) {
							String name = rs.getString(1);
							long lastreset = rs.getLong(2);
	
							// If table is still valid - then use that (minus the CACHE.)
							if (System.currentTimeMillis() < lastreset + getExpiryDuration()) cacheTableName = name.substring(6);
	
							// Note: keep looping in case one is more valid (?)
						}
						rs.close();
					}
				} finally {
					if (null != stmt) {
						stmt.close();
						recyclePooledLocalDerbyConnection(conn);
					}
				}

			} catch (Exception e) {
				logger.logException( GDBMessages.DSWRAPPER_DURABLE_CACHE_LOOKUP_ERROR, "Unable to check for existing valid durable cache table to use.", e);
			}
		}

		// If still null - use default 'super + _ + #' (to make it unique)
		if (cacheTableName == null)
			cacheTableName = super.getCacheTableName() + "_" + cacheTableSuffix.incrementAndGet();

		return cacheTableName;
	}

	/**
	 * Mark the 'logical' usage state of the cache. This allows us to ensure
	 * that in-use caches are not deleted inappropriately.
	 * 
	 * @param inUse - boolean - whether the cache is now in use or not.
	 */
	public void markCacheInUse(boolean inUse) {
		synchronized (cacheUseCountMap) {
			String cacheTableName = getCacheSchemaAndTableName();

			AtomicLong cacheUseCount;

			// If the cache is now in use - update use count - adding into the table if necessary
			if (inUse) {
				if (cacheUseCountMap.containsKey(cacheTableName)) {
					cacheUseCount = cacheUseCountMap.get(cacheTableName);
				} else {
					cacheUseCount = new AtomicLong(0);
					cacheUseCountMap.put(cacheTableName, cacheUseCount);
				}

				cacheUseCount.getAndIncrement();
			}
			// Else the cache is no longer in use - decrement (only if already in the table) (don't decrement to less than 0)
			else {
				if (cacheUseCountMap.containsKey(cacheTableName)) {
					cacheUseCount = cacheUseCountMap.get(cacheTableName);

					if (cacheUseCount.getAndDecrement() == 0) {
						cacheUseCount.set(0);
					}
				}
			}
		}
	}

	/**
	 * Gets the modify lock for this cache. No-one should attempt to modify the
	 * cache without first retrieving and then obtaining this lock. This ensures
	 * that there is no undesirable concurrent modification.
	 * 
	 * @return The cache modification {@link Lock}.
	 */
	public Lock getCacheModifyLock() {
		Lock lock;

		String cacheTableName = getCacheSchemaAndTableName();

		synchronized (cacheModifyLockMap) {
			if (cacheModifyLockMap.containsKey(cacheTableName)) {
				lock = cacheModifyLockMap.get(cacheTableName);
			} else {
				lock = new ReentrantLock();
				cacheModifyLockMap.put(cacheTableName, lock);
			}
		}

		return lock;
	}

	/**
	 * Marks the current durable cache as invalid.
	 */
	public void invalidateCache() {
		
		// Set lastreset to -1 in CACHE.EXPIRES - so no-one else tries to use it.
		
		logger.logImportant("Cache: '" + getCacheTableName() + "' flagged as invalid.");

		try {
			Connection conn = null;
			Statement stmt = null;

			try {
				conn = getPooledLocalDerbyConnection();
				stmt = conn.createStatement();
				stmt.executeUpdate("UPDATE CACHE.EXPIRES SET lastreset = -1 WHERE name = " + getCacheSchemaAndTableName());

			} finally {
				if (null != stmt) {
					stmt.close();
					recyclePooledLocalDerbyConnection(conn);
				}
			}

		} catch (Exception e) {
			logger.logException( GDBMessages.DSWRAPPER_DURABLE_CACHE_INVALIDATE_ERROR, "Unable to invalidate durable cache table: '"
					+ getCacheSchemaAndTableName() + "'.", e);
		}

		isCached = -1;
	}

	/**
	 * Performs clean up of all completed and redundant caches that are no
	 * longer in use. Children should call this from their close method to
	 * ensure the cache is suitably maintained.
	 */
	public void cleanUpCaches() {
		synchronized (cacheUseCountMap) {

			logger.logInfo("Performing cache clean up.");

			Set<Entry<String, AtomicLong>> cUCEntries = cacheUseCountMap.entrySet();

			// Loop through all of them
			Iterator<Entry<String, AtomicLong>> cUCIterator = cUCEntries.iterator();

			while (cUCIterator.hasNext()) {
				Map.Entry<String, AtomicLong> entry = (Map.Entry<String, AtomicLong>) cUCIterator.next();

				// If no current uses - then candidate for cleaning
				if (entry.getValue().get() == 0) {

					String queryId = (String) entry.getKey();

					// Get the lock object for 'this' query
					Lock lock = null;

					synchronized (cacheModifyLockMap) {
						if (cacheModifyLockMap.containsKey(queryId)) {
							lock = cacheModifyLockMap.get(queryId);
						}
					}

					// Now try to get the lock, so we can sync on the cache - to avoid messing with other people
					if (lock != null && lock.tryLock()) {

						try {
							Statement stmt = null;
							Connection c = null;

							try {
								c = getPooledLocalDerbyConnection();
								stmt = c.createStatement();

								ResultSet rs = stmt.executeQuery("SELECT lastreset FROM CACHE.EXPIRES WHERE name = '" + queryId + "'");

								if (!rs.next())
									throw new Exception( "No expiry entry found for cache table: " + queryId);

								long lastreset = rs.getLong(1);
								rs.close();

								// If table has expired
								if (System.currentTimeMillis() > lastreset + getExpiryDuration()) {

									logger.logInfo("Cache table: '" + queryId + "' has expired.");

									if ( false == Util.isExistsDerbyTable(c, "CACHE", queryId.substring(6)) ) {
										logger.logInfo("Unable to drop cache table as it does not exist: " + queryId);
										return;
									}
									stmt = c.createStatement();

									// Note: don't attempt drop of table, as this often hangs for some reason
									stmt.execute("DELETE FROM " + queryId);

									/*
									 * Note: can only delete from cache.expires as table names have incrementing suffix and the cache is cleared 
									 * on restart. If that wasn't the case, the default AbstractVTI.findOrCreateCacheTableAndExpiryEntry() logic
									 * falls down (because of the order it does things in).
									 */
									stmt.execute("DELETE FROM CACHE.EXPIRES WHERE name = '" + queryId + "'");

									// Remove the query from the cacheUsageCount map
									cUCIterator.remove();

									logger.logInfo("Cache table: '" + queryId + "' successfully deleted.");

									// Note: we leave it in the lock map
								}
							} catch (Exception e) {
								e.printStackTrace();
								logger.logWarning( GDBMessages.DSWRAPPER_DURABLE_CACHE_DELETE_WARNING, "Unable to drop cache table " + queryId + ": " + e);
							} finally {

								try {
									logger.logInfo("Closing stmt isNull? " + (null == stmt)
													+ ", and recycling its connection isActive? " + (null != c && !c.isClosed()));

									if (null != stmt) stmt.close();
									if (null != c && !c.isClosed()) recyclePooledLocalDerbyConnection(c);
								} catch (SQLException e) {
									e.printStackTrace();
									logger.logWarning( GDBMessages.DSWRAPPER_DURABLE_CACHE_RECYCLE_CONNECTION_ERROR,
													"Unable to recycle connection after dropping cache table.");
								}
							}
						} finally {
							// Whatever happens, make sure we release the lock - else bad things.
							lock.unlock();
						}
					}
				}
			}

			logger.logInfo("Cache clean up complete.");
		}
	}
}
