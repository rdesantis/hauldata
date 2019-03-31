/*
 * Copyright (c) 2016, 2018, Ronald DeSantis
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */

package com.hauldata.dbpa.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class DatabaseConnection extends com.hauldata.dbpa.connection.Connection {

	private int activeConnectionCount = 0;
	private List<Connection> idleConnections = new LinkedList<Connection>();

	private static Boolean allConnections = true;

	@Override
	public void setProperties(Properties properties) {

		// Close the idle connections so on next usage, connections are re-established with new properties.

		if (activeConnectionCount > 0) {
			throw new RuntimeException("Cannot change connection while it is in use.");
		}

		try { assureClosed(); } catch (Exception ex) {}

		super.setProperties(properties);
	}

	/**
	 * Return JDBC connection, setting it up if required
	 * using properties specified on setProperties(Properties).
	 * @return the JDBC connection or null if setProperties(Properties)
	 * was not called.  Throws an exception if the connection cannot
	 * be established.
	 */
	public Connection get() {

		Connection conn = null;

		synchronized (this) {

			if (idleConnections.size() > 0) {
				conn = assureValid(idleConnections.remove(0));
			}

			if ((conn == null) && (getProperties() != null)) {

				setLongSleepSeconds("longSleepSeconds");

				String driver = getProperties().getProperty("driver");
				String url = getProperties().getProperty("url");

				if (driver == null || url == null) {
					throw new RuntimeException("Required database connection properties are not set");
				}

				// This section is found to cause a process to hang if multiple concurrent threads enter it
				// specifying different drivers.

				synchronized (allConnections) {
					try {
						Class.forName(driver);
						conn = DriverManager.getConnection(url, getProperties());
					}
					catch (Exception ex) {
						throw new RuntimeException("Database connection failed: " + ex.toString(), ex);
					}
				}
			}

			if (conn != null) {
				++activeConnectionCount;
			}
		}

		return conn;
	}

	/**
	 * Test connection for validity.  If not valid, close it.
	 *
	 * @param conn is the connection to test
	 * @return conn if valid, otherwise null
	 */
	private Connection assureValid(Connection conn) {

		boolean isValid = false;
		try { isValid = conn.isValid(0); } catch (Exception ex) {}
		if (!isValid) {
			try { conn.close(); } catch (Exception ex) {}
			conn = null;
		}
		return conn;
	}

	/**
	 * Indicate that a JDBC connection acquired with getConnection()
	 * is no longer needed by the acquirer.
	 */
	public void release(Connection conn) {

		synchronized (this) {
			idleConnections.add(conn);
			--activeConnectionCount;
		}
	}

	/**
	 * Close all idle JDBC connections.  It is assumed there are no active connections.
	 * @throws SQLException
	 */
	public void assureClosed() throws SQLException {

		for (Connection conn: idleConnections) {
			conn.close();
		}
		idleConnections.clear();
	}

	// Sleep functions.  These are intended to release the database connection
	// when all active tasks are in a long sleep period.
	
	private static final int defaultLongSleepSeconds = 120;
	private long longSleepMillis;

	private void setLongSleepSeconds(String propName) {
		String longSleepSecondsString = getProperties().getProperty(propName, String.valueOf(defaultLongSleepSeconds));
		int longSleepSeconds = defaultLongSleepSeconds;
		try { longSleepSeconds = Integer.parseInt(longSleepSecondsString); } catch (Exception ex) {}
		longSleepMillis = longSleepSeconds * 1000L;
	}

	/**
	 * Alerts the context that a task will begin to sleep.
	 * 
	 * @param millis is the approximate number of milliseconds for the sleep.
	 * @return an indicator of whether the context considers the sleep
	 * to be a "long" sleep.  This value must be passed to wakeFromSleep(boolean)
	 * when the sleep has finished. 
	 * @see DatabaseConnection#wakeFromSleep(boolean)
	 */
	public boolean prepareToSleep(long millis) {

		if ((activeConnectionCount == 0) || (millis < longSleepMillis)) {
			return false;
		}

		synchronized (this) {
			if (activeConnectionCount == 0) {
				try { assureClosed(); } catch (Exception ex) {}
			}
		}

		return true;
	}

	/**
	 * Alerts the context that a task has awakened from sleep.
	 * 
	 * @param longSleep is the return value from a previous call to prepareToSleep(long).
	 * @see DatabaseConnection#prepareToSleep(long)
	 */
	public void wakeFromSleep(boolean longSleep) {

		// Before a long sleep, all idle database connections are closed.
		// But even for a short sleep, it is assumed that a database connections may become
		// invalid during that time.  However, get() always checks that an idle connection
		// is valid before using it, discards it, and creates another if necessary, so
		// no action is needed here.
	}
}
