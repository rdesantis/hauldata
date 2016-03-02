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

package com.hauldata.dbpa.process;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;

import com.hauldata.dbpa.loader.Loader;
import com.hauldata.dbpa.log.Logger;
import com.hauldata.dbpa.log.NullLogger;

/**
 * Context in which a task will run.
 */
public class Context {

	public Properties connectionProps;
	public Properties sessionProps;
	public Properties ftpProps;
	public String dataPath;
	public Loader loader;
	public Logger logger;
	public TaskExecutor executor;
	public TaskExecutor rootExecutor;

	public Files files;

	private Path dataParent;

	private static class Resources {
		public Connection conn;
		public Session session;
		
		public int connCount;
		public LocalDateTime connLastUsed;

		Resources() {
			conn = null;
			session = null;

			connCount = 0;
			connLastUsed = null;
		}
	}

	private Resources resources;
	
	/**
	 * Constructor for context in which a process will run.  Fields are:
	 * - connectionProps are the properties to use when setting up a database connection for the process
	 * - sessionProps are properties to use when setting up a JavaMail session for the process
	 * - ftpProps are properties to use when setting up an FTP server connection for the process
	 * - dataPath is the path for data files
	 * - loader is the factory to instantiate nested processes
	 * 
	 * In addition, the following public data member should be set BY THE CALLER AFTER the context has been constructed.
	 * If not set by the caller, it remains as set by the constructor to a null log that discards all logging.
	 * The log is set separately from the context constructor because the log's constructor may take the context as an argument.  
	 * - logger is the log where task status messages can be written
	 * 
	 * The above are all exposed as public data members.  In addition, the following public data members
	 * are instantiated by the constructor:
	 * - files is the set of files written and/or read by the process
	 * - executor is task executor to which addition concurrent tasks can be submitted
	 */
	public Context(Properties connectionProps, Properties sessionProps, Properties ftpProps, String dataPath, Loader loader) {

		this.connectionProps = connectionProps;
		this.sessionProps = sessionProps;
		this.ftpProps = ftpProps;
		this.dataPath = dataPath;
		this.loader = loader;

		logger = NullLogger.logger;

		files = new Files();

		executor = new TaskExecutor();
		rootExecutor = executor;

		dataParent = Files.getPath(dataPath);

		resources = new Resources();
	}

	/**
	 * Close the context.
	 */
	public void close() {
		try { executor.close(); } catch (Exception ex) {}
		try { files.assureAllClosed(); } catch (Exception ex) {}
		try { logger.close(); } catch (Exception ex) {}
		try { assureConnectionClosed(); } catch (Exception ex) {}
	}

	/**
	 * Construct a context to use by a process nested within this process.
	 * @return the nested process context.  It shares the JDBC connection and JavaMail session
	 * of this parent context.  The log data member must be set on the nested context.
	 */
	public Context nestContext() {
		
		Context context = new Context(connectionProps, sessionProps, ftpProps, dataPath, loader);

		context.executor = new TaskExecutor();
		context.rootExecutor = context.executor;

		context.resources = resources;

		return context;
	}

	/**
	 * Close a nested context.
	 */
	public void closeNested() {
		try { executor.close(); } catch (Exception ex) {}
		try { files.assureAllClosed(); } catch (Exception ex) {}
		try { logger.close(); } catch (Exception ex) {}
	}

	/**
	 * Construct a context to use by a task set nested within this process.
	 * @return the nested task set context.  It has a distinct task executor but
	 * otherwise shares all data members of this parent context but can be given
	 * a different log data member if desired.
	 */
	public Context cloneContext() {
		
		Context context = new Context(connectionProps, sessionProps, ftpProps, dataPath, loader);

		context.logger = logger;
		context.files = files;

		context.executor = new TaskExecutor();
		context.rootExecutor = rootExecutor;

		context.dataParent = dataParent;
		context.resources = resources;

		return context;
	}

	/**
	 * Close a cloned context.
	 */
	public void closeCloned() {
		try { executor.close(); } catch (Exception ex) {}
	}

	// Database connection functions.

	/**
	 * Return JDBC connection, setting it up if required
	 * using properties specified on the constructor.
	 * @return the JDBC connection or null if JDBC properties were not
	 * provided on the constructor.  Throws an exception if the
	 * connection cannot be established.
	 */
	public Connection getConnection() {

		synchronized (resources) {
			if (resources.conn != null) {

				if (
						(resources.connCount == 0) &&
						(resources.connLastUsed != null) &&
						(resources.connLastUsed.until(LocalDateTime.now(), ChronoUnit.MILLIS) > longSleepMillis)) {

					wakeFromSleep(true);
				}

				++resources.connCount;
			}
			else if (connectionProps != null) {

				setLongSleepSeconds("longSleepSeconds");

				String driver = connectionProps.getProperty("driver");
				String url = connectionProps.getProperty("url");
				try {
					Class.forName(driver);
					resources.conn = DriverManager.getConnection(url, connectionProps);
					resources.connCount = 1;
				}
				catch (Exception ex) {
					throw new RuntimeException("Database connection failed: " + ex.toString(), ex);
				}
			}
			
			if (resources.conn != null) {
				resources.connLastUsed = LocalDateTime.now();
			}
		}
		return resources.conn;
	}

	/**
	 * Indicate that a JDBC connection acquired with getConnection()
	 * is no longer needed by the acquirer.
	 */
	public void releaseConnection() {

		synchronized (resources) {
			resources.connLastUsed = LocalDateTime.now();
			--resources.connCount;
		}
	}

	public void assureConnectionClosed() throws SQLException {
		if (resources.conn != null) {
			resources.conn.close();
			resources.conn = null;
		}
	}

	// Sleep functions.  These are intended to release the database connection
	// when all active tasks are in a long sleep period.
	
	private static final int defaultLongSleepSeconds = 120;
	private long longSleepMillis;

	private void setLongSleepSeconds(String propName) {
		String longSleepSecondsString = connectionProps.getProperty(propName, String.valueOf(defaultLongSleepSeconds));
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

		if ((resources.conn == null) || (millis < longSleepMillis)) {
			return false;
		}

		synchronized (resources) {
			if (resources.connCount == 0) {
				try { assureConnectionClosed(); } catch (Exception ex) {}
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
		
		synchronized (resources) {
			if ((resources.conn != null) && (resources.connCount == 0)) {
				boolean isValid = false;
				try { isValid = resources.conn.isValid(0); } catch (Exception ex) {}
				if (!isValid) {
					try { resources.conn.close(); } catch (Exception ex) {}
					resources.conn = null;
				}
			}
		}
	}

	/**
	 * Return email server session, setting it up if required
	 * using properties specified on the constructor.
	 * @return the email session or null if session properties were not
	 * provided on the constructor.  Throws an exception if the
	 * session cannot be established.
	 */
	public Session getSession() {

		synchronized (resources) {
			if (resources.session == null && sessionProps != null) {
	
				String user = sessionProps.getProperty("user");
				String password = sessionProps.getProperty("password");
				
				try {
					resources.session = Session.getInstance(sessionProps, new PasswordAuthenticator(user, password));
				}
				catch (Exception ex) {
					throw new RuntimeException("Email session setup failed: " + ex.toString(), ex);
				}
			}
		}
		return resources.session;
	}

	private static class PasswordAuthenticator extends Authenticator {

		PasswordAuthenticator(String user, String password) {
			this.user = user;
			this.password = password;
		}

		protected PasswordAuthentication getPasswordAuthentication() {
			return new PasswordAuthentication(user, password);
		}
		
		private String user;
		private String password;
	}

	// File system functions.

	/**
	 * Get the fully-resolved path of a file name that may include a relative or absolute path.
	 * Uses the dataPath of the context to resolve relative paths.
	 * @param fileName is the file name to resolve.
	 * @return the fully-resolved path
	 */
	public Path getDataPath(String fileName) {
		return dataParent.resolve(fileName).toAbsolutePath().normalize();
	}
}
