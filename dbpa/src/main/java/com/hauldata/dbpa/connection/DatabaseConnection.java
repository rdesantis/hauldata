/*
 * Copyright (c) 2016, Ronald DeSantis
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
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class DatabaseConnection extends com.hauldata.dbpa.connection.Connection {

	private Connection conn;

	private int connCount = 0;
	private LocalDateTime connLastUsed = null;
	
	@Override
	public void setProperties(Properties properties) {

		// Close the connection so on next usage, connection is re-established with new properties.

		if (connCount > 0) {
			throw new RuntimeException("Cannot change connection while it is in use.");
		}

		try { assureClosed(); } catch (Exception ex) {}

		super.setProperties(properties);
	}

	/**
	 * Return JDBC connection, setting it up if required
	 * using properties specified on setProperties(Properties).
	 * @return the JDBC connection or setProperties(Properties)
	 * was not called.  Throws an exception if the
	 * connection cannot be established.
	 */
	public Connection get() {

		synchronized (this) {
			if (conn != null) {

				if (
						(connCount == 0) &&
						(connLastUsed != null) &&
						(connLastUsed.until(LocalDateTime.now(), ChronoUnit.MILLIS) > longSleepMillis)) {

					wakeFromSleep(true);
				}

				++connCount;
			}
			else if (getProperties() != null) {

				setLongSleepSeconds("longSleepSeconds");

				String driver = getProperties().getProperty("driver");
				String url = getProperties().getProperty("url");

				if (driver == null || url == null) {
					throw new RuntimeException("Required database connection properties are not set");
				}

				try {
					Class.forName(driver);
					conn = DriverManager.getConnection(url, getProperties());
					connCount = 1;
				}
				catch (Exception ex) {
					throw new RuntimeException("Database connection failed: " + ex.toString(), ex);
				}
			}
			
			if (conn != null) {
				connLastUsed = LocalDateTime.now();
			}
		}
		return conn;
	}

	/**
	 * Indicate that a JDBC connection acquired with getConnection()
	 * is no longer needed by the acquirer.
	 */
	public void release() {

		synchronized (this) {
			connLastUsed = LocalDateTime.now();
			--connCount;
		}
	}

	public void assureClosed() throws SQLException {
		if (conn != null) {
			conn.close();
			conn = null;
		}
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
	 * @see Context#wakeFromSleep(boolean)
	 */
	public boolean prepareToSleep(long millis) {

		if ((conn == null) || (millis < longSleepMillis)) {
			return false;
		}

		synchronized (this) {
			if (connCount == 0) {
				try { assureClosed(); } catch (Exception ex) {}
			}
		}

		return true;
	}

	/**
	 * Alerts the context that a task has awakened from sleep.
	 * 
	 * @param longSleep is the return value from a previous call to prepareToSleep(long).
	 * @see Context#prepareToSleep(long)
	 */
	public void wakeFromSleep(boolean longSleep) {

		// If the task went into a long sleep, the database connection may have been
		// closed at sleep time but may have been re-opened during the sleep if another
		// task became active.  Even for a short sleep, it is assumed that an
		// indefinite number of short sleeps may occur over an extended period
		// of time and that the database connection may become invalid during that time.
		// Therefore, the connection is tested for validity.  If found to be invalid,
		// it is closed so that a new connection will be created by the next call to
		// getConnection().
		
		synchronized (this) {
			if ((conn != null) && (connCount == 0)) {
				boolean isValid = false;
				try { isValid = conn.isValid(0); } catch (Exception ex) {}
				if (!isValid) {
					try { conn.close(); } catch (Exception ex) {}
					conn = null;
				}
			}
		}
	}
}
