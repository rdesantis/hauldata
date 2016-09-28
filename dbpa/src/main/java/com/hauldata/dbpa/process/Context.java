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
import java.sql.SQLException;
import java.util.Properties;

import javax.mail.Session;

import org.apache.commons.vfs2.FileSystemException;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.connection.FtpConnection;
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
	public Properties pathProps;
	public Loader loader;
	public Logger logger;
	public TaskExecutor executor;
	public TaskExecutor rootExecutor;

	public Files files;

	private Path readParent;
	private Path writeParent;

	private static class Resources {
		public DatabaseConnection dbconn;
		public EmailConnection mailconn;
		public FtpConnection ftpconn;
		
		Resources() {
			dbconn = null;
			mailconn = null;
			ftpconn = null;
		}
	}

	private Resources resources;
	
	/**
	 * Constructor for context in which a process will run.  Fields are:
	 * - connectionProps are the properties to use when setting up a database connection for the process
	 * - sessionProps are properties to use when setting up a JavaMail session for the process
	 * - ftpProps are properties to use when setting up an FTP server connection for the process
	 * - pathProps contain the default paths to use when reading and writing files
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
	public Context(Properties connectionProps, Properties sessionProps, Properties ftpProps, Properties pathProps, Loader loader) {

		this.connectionProps = connectionProps;
		this.sessionProps = sessionProps;
		this.ftpProps = ftpProps;
		this.pathProps = pathProps;
		this.loader = loader;

		logger = NullLogger.logger;

		files = new Files();

		executor = new TaskExecutor();
		rootExecutor = executor;

		readParent = getParent(pathProps, "read");
		writeParent = getParent(pathProps, "write");

		resources = new Resources();
		resources.dbconn = new DatabaseConnection();
		resources.dbconn.setProperties(connectionProps);
		resources.mailconn = new EmailConnection();
		resources.mailconn.setProperties(sessionProps);
		resources.ftpconn = new FtpConnection();
		resources.ftpconn.setProperties(ftpProps);
	}

	private static Path getParent(Properties pathProps, String mode) {
		String location = ".";
		if (pathProps != null) {
			location = pathProps.getProperty(mode, pathProps.getProperty("data", location));
		}
		return Files.getPath(location);
	}

	/**
	 * Close the context.
	 */
	public void close() {
		try { executor.close(); } catch (Exception ex) {}
		try { files.assureAllClosed(); } catch (Exception ex) {}
		try { logger.close(); } catch (Exception ex) {}
		try { resources.dbconn.assureClosed(); } catch (Exception ex) {}
	}

	/**
	 * Construct a context to use by a process nested within this process.
	 * @return the nested process context.  It shares the JDBC connection and JavaMail session
	 * of this parent context.  The log data member must be set on the nested context.
	 */
	public Context nestContext() {
		
		Context context = new Context(connectionProps, sessionProps, ftpProps, pathProps, loader);

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
		
		Context context = new Context(connectionProps, sessionProps, ftpProps, pathProps, loader);

		context.logger = logger;
		context.files = files;

		context.executor = new TaskExecutor();
		context.rootExecutor = rootExecutor;

		context.readParent = readParent;
		context.writeParent = writeParent;
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
		return resources.dbconn.get();
	}

	/**
	 * Indicate that a JDBC connection acquired with getConnection()
	 * is no longer needed by the acquirer.
	 */
	public void releaseConnection() {
		resources.dbconn.release();
	}

	public void assureConnectionClosed() throws SQLException {
		resources.dbconn.assureClosed();
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
		return resources.dbconn.prepareToSleep(millis);
	}

	/**
	 * Alerts the context that a task has awakened from sleep.
	 * 
	 * @param longSleep is the return value from a previous call to prepareToSleep(long).
	 * @see Context#prepareToSleep(long)
	 */
	public void wakeFromSleep(boolean longSleep) {
		resources.dbconn.wakeFromSleep(longSleep);
	}

	// Email connection functions.

	/**
	 * Return email server session for the indicated connection if not null,
	 * otherwise for the context default connection.
	 * @return the email session or null if session properties were not provided.
	 * Throws an exception if the session cannot be established.
	 */
	public Session getSession(EmailConnection connection) {
		return (connection != null ? connection : resources.mailconn).get();
	}

	// FTP connection functions.

	/**
	 * Return FTP manager for the indicated connection if not null,
	 * otherwise for the context default connection.
	 */
	public FtpConnection.Manager getManager(FtpConnection connection, boolean isBinary) throws FileSystemException {
		return (connection != null ? connection : resources.ftpconn).getManager(isBinary);
	}

	// File system functions.

	/**
	 * Get the fully-resolved path of a file name that may include a relative or absolute path.
	 * Uses the readPath of the context to resolve relative paths.
	 * @param fileName is the file name to resolve.
	 * @return the fully-resolved path
	 */
	public Path getReadPath(String fileName) {
		return readParent.resolve(fileName).toAbsolutePath().normalize();
	}
	public Path getWritePath(String fileName) {
		return writeParent.resolve(fileName).toAbsolutePath().normalize();
	}
	public Path getDataPath(String fileName, boolean writeNotRead) {
		return writeNotRead ? getWritePath(fileName) : getReadPath(fileName);
	}
}
