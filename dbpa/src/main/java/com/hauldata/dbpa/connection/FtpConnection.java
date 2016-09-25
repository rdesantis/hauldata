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

import java.util.Properties;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;

public class FtpConnection extends Connection {

	public class Manager {
		
		private FileSystemOptions options = null;
		private StandardFileSystemManager manager = null;

		Manager(boolean isBinary) throws FileSystemException {
			
			options = getOptions(isBinary);

			manager = new StandardFileSystemManager();
			manager.init();
		}

		public void copy(String localFileName, String remoteFileName) throws FileSystemException {
			
			FileObject localFile = manager.resolveFile(localFileName);

			String remoteFileURI = getRemoteFileURI(remoteFileName);

			FileObject remoteFile = manager.resolveFile(remoteFileURI, options);

			remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
		}

		public void close() {

			if (manager != null) {
				manager.close();
			}

			manager = null;
			options = null;
		}

		private FileSystemOptions getOptions(boolean isBinary) {

			if (options == null) {
				String protocol = getProperties().getProperty("protocol").trim();
				if (protocol.equals("ftp") || protocol.equals("ftps")) {
					options = getFTPOptions(isBinary);
				}
				else if (protocol.equals("sftp")) {
					options = getSFTPOptions(isBinary);
				}
				else {
					throw new RuntimeException("File transfer protocol is not supported: " + protocol);
				}
			}

			return options;
		}

	}

	@Override
	public void setProperties(Properties properties) {
		super.setProperties(properties);
	}

	// See https://commons.apache.org/proper/commons-vfs/index.html
	// See http://www.mysamplecode.com/2013/06/sftp-apache-commons-file-download.html

	public Manager getManager(boolean isBinary) throws FileSystemException {

		return new Manager(isBinary);
	}

	private FileSystemOptions getFTPOptions(boolean isBinary) {

		String passiveModeString = getProperties().getProperty("mode", "yes").trim();
		String dataTimeoutString = getProperties().getProperty("dataTimeout").trim();
		String socketTimeoutString = getProperties().getProperty("socketTimeout").trim();
//		String connectTimeoutString = getProperties().getProperty("connectTimeout").trim();

		FileSystemOptions options = new FileSystemOptions();

		FtpFileSystemConfigBuilder configBuilder = FtpFileSystemConfigBuilder.getInstance();
		configBuilder.setUserDirIsRoot(options, false);

		configBuilder.setPassiveMode(options, Boolean.parseBoolean(passiveModeString));
		if (dataTimeoutString != null) {
			configBuilder.setDataTimeout(options, Integer.parseInt(dataTimeoutString)); 
		}
		if (socketTimeoutString != null) {
			configBuilder.setSoTimeout(options, Integer.parseInt(socketTimeoutString)); 
		}
// The following are available in VFS2.1:
//		if (connectTimeoutString != null) {
//			configBuilder.setConnectTimeout(options, Integer.parseInt(connectTimeoutString)); 
//		}
//		configBuilder.setFileType(opts, isBinary ? FtpFileType.BINARY : FtpFileType.ASCII);

		return options;
	}

	private FileSystemOptions getSFTPOptions(boolean isBinary) {

		String strictHostKeyChecking = getProperties().getProperty("strictHostKeyChecking", "no").trim();
		String timeoutString = getProperties().getProperty("timeout").trim();

		FileSystemOptions options = new FileSystemOptions();

		try {
			SftpFileSystemConfigBuilder configBuilder = SftpFileSystemConfigBuilder.getInstance();
			configBuilder.setUserDirIsRoot(options, false);

			configBuilder.setStrictHostKeyChecking(options, strictHostKeyChecking);
			if (timeoutString != null) {
				configBuilder.setTimeout(options, Integer.parseInt(timeoutString));
			}
		}
		catch (FileSystemException ex) {
			throw new RuntimeException("Error configuring SFTP options: " + ex.getMessage());
		}

		return options;
	}

	private String getRemoteFileURI(String remoteFileName) {

		String protocol = getProperties().getProperty("protocol").trim();
		String hostname = getProperties().getProperty("hostname").trim();
		String user = getProperties().getProperty("user").trim();
		String password = getProperties().getProperty("password").trim();
		
		String cleanedFileName = remoteFileName.replaceAll(" ", "%20");

		return protocol + "://" + user + ":" + password +  "@" + hostname + cleanedFileName;
	}

	static { quietLog4j(); }
	static private boolean quietLog4jCalled = false;
	private static void quietLog4j() {
		if (!quietLog4jCalled) {
			org.apache.log4j.BasicConfigurator.configure(new org.apache.log4j.varia.NullAppender());
			quietLog4jCalled = true;
		}
	}
}
