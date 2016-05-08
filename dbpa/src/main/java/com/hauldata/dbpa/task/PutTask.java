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

package com.hauldata.dbpa.task;

import java.nio.file.Path;
import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class PutTask extends Task {

	public PutTask(
			Prologue prologue,
			boolean isBinary,
			List<Expression<String>> localNames,
			Expression<String> remoteName) {
		
		super(prologue);
		this.isBinary = isBinary;
		this.localNames = localNames;
		this.remoteName = remoteName;
		
		options = null;
		
		quietLog4jCalled = false;
	}

	@Override
	protected void execute(Context context) {

		quietLog4j();
		
		// See https://commons.apache.org/proper/commons-vfs/index.html
		// See http://www.mysamplecode.com/2013/06/sftp-apache-commons-file-download.html

		String remotePathName = remoteName.evaluate();
		
		FileSystemOptions options = getOptions(context, isBinary);

		StandardFileSystemManager manager = null;
		try {
			manager = new StandardFileSystemManager();
			manager.init();

			for (Expression<String> localName : localNames) {

				Path localFilePath = context.getWritePath(localName.evaluate());
				context.files.assureNotOpen(localFilePath);

				FileObject localFile = manager.resolveFile(localFilePath.toString());

				String remoteFileURI = getRemoteFileURI(context, remotePathName + "/" + localFilePath.getFileName().toString());

				FileObject remoteFile = manager.resolveFile(remoteFileURI, options);

				remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
			}
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to transfer file: " + message, ex);
		}
		finally {
			if (manager != null) manager.close();
		}
	}

	private FileSystemOptions getOptions(Context context, boolean isBinary) {

		if (options == null) {
			String protocol = context.ftpProps.getProperty("protocol").trim();
			if (protocol.equals("ftp") || protocol.equals("ftps")) {
				options = getFTPOptions(context, isBinary);
			}
			else if (protocol.equals("sftp")) {
				options = getSFTPOptions(context, isBinary);
			}
			else {
				throw new RuntimeException("File transfer protocol is not supported: " + protocol);
			}
		}
		return options;
	}

	private FileSystemOptions getFTPOptions(Context context, boolean isBinary) {

		String passiveModeString = context.ftpProps.getProperty("mode", "yes").trim();
		String dataTimeoutString = context.ftpProps.getProperty("dataTimeout").trim();
		String socketTimeoutString = context.ftpProps.getProperty("socketTimeout").trim();
//		String connectTimeoutString = context.ftpProps.getProperty("connectTimeout").trim();

		options = new FileSystemOptions();

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
//			configBuilder.setConnectTimeout(opts, Integer.parseInt(connectTimeoutString)); 
//		}
//		configBuilder.setFileType(opts, isBinary ? FtpFileType.BINARY : FtpFileType.ASCII);

		return options;
	}

	private FileSystemOptions getSFTPOptions(Context context, boolean isBinary) {

		String strictHostKeyChecking = context.ftpProps.getProperty("strictHostKeyChecking", "no").trim();
		String timeoutString = context.ftpProps.getProperty("timeout").trim();

		options = new FileSystemOptions();

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

	private String getRemoteFileURI(Context context, String remoteFileName) {

		String protocol = context.ftpProps.getProperty("protocol").trim();
		String hostname = context.ftpProps.getProperty("hostname").trim();
		String user = context.ftpProps.getProperty("user").trim();
		String password = context.ftpProps.getProperty("password").trim();
		
		String cleanedFileName = remoteFileName.replaceAll(" ", "%20");

		return protocol + "://" + user + ":" + password +  "@" + hostname + cleanedFileName;
	}

	private void quietLog4j() {
		if (!quietLog4jCalled) {
			org.apache.log4j.BasicConfigurator.configure(new org.apache.log4j.varia.NullAppender());
			quietLog4jCalled = true;
		}
	}

	private boolean isBinary;
	private List<Expression<String>> localNames;
	private Expression<String> remoteName;
	
	private FileSystemOptions options;
	
	private boolean quietLog4jCalled;
}
