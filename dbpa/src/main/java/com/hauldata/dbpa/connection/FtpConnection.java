/*
 * Copyright (c) 2016, 2018-2019, Ronald DeSantis
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

		public boolean isRemoteDirectory(String name) throws FileSystemException {

			String remoteFileURI = getRemoteFileURI(name);
			FileObject remote = manager.resolveFile(remoteFileURI, options);

			return remote.isFolder();
		}

		public void copyLocalToRemote(String localFileName, String remoteFileName) throws FileSystemException {

			ResolvedFiles files = new ResolvedFiles(localFileName, remoteFileName);

			files.remote.copyFrom(files.local, Selectors.SELECT_SELF);
		}

		public void copyLocalFromRemote(String localFileName, String remoteFileName) throws FileSystemException {

			ResolvedFiles files = new ResolvedFiles(localFileName, remoteFileName);

			files.local.copyFrom(files.remote, Selectors.SELECT_SELF);
		}

		private class ResolvedFiles {
			FileObject local;
			FileObject remote;

			public ResolvedFiles(String localFileName, String remoteFileName) throws FileSystemException {

				local = manager.resolveFile(localFileName);

				String remoteFileURI = getRemoteFileURI(remoteFileName);

				remote = manager.resolveFile(remoteFileURI, options);
			}
		}

		public void moveRemoteToRemote(String fromFileName, String toFileName) throws FileSystemException {

			String fromFileURI = getRemoteFileURI(fromFileName);
			FileObject from = manager.resolveFile(fromFileURI, options);

			String toFileURI = getRemoteFileURI(toFileName);
			FileObject to = manager.resolveFile(toFileURI, options);

			from.moveTo(to);
		}

		public void deleteRemote(String fileName) throws FileSystemException {

			String fileURI = getRemoteFileURI(fileName);
			FileObject file = manager.resolveFile(fileURI, options);

			file.delete();
		}

		public List<String> findRemote(String baseFolder, String fileNamePattern) throws FileSystemException {

			String regex = fileNamePattern
					.replace(".", "\\.")
					.replace("*", ".*")
					.replace("?", ".");
			Pattern pattern = Pattern.compile(regex);

			FileObject[] children = manager.resolveFile(getRemoteFileURI(baseFolder), options).getChildren();

			return Arrays.stream(children)
					.map(fo -> fo.getName().getBaseName())
					.filter(name -> pattern.matcher(name).matches())
					.collect(Collectors.toList());
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

		String passiveModeString = getProperties().getProperty("passiveMode", "true");
		String dataTimeoutString = getProperties().getProperty("dataTimeout");
		String socketTimeoutString = getProperties().getProperty("socketTimeout");
//		String connectTimeoutString = getProperties().getProperty("connectTimeout");

		FileSystemOptions options = new FileSystemOptions();

		FtpFileSystemConfigBuilder configBuilder = FtpFileSystemConfigBuilder.getInstance();
		configBuilder.setUserDirIsRoot(options, false);

		configBuilder.setPassiveMode(options, Boolean.parseBoolean(passiveModeString.trim()));
		if (dataTimeoutString != null) {
			configBuilder.setDataTimeout(options, Integer.parseInt(dataTimeoutString.trim()));
		}
		if (socketTimeoutString != null) {
			configBuilder.setSoTimeout(options, Integer.parseInt(socketTimeoutString.trim()));
		}
// The following are available in VFS2.1:
//		if (connectTimeoutString != null) {
//			configBuilder.setConnectTimeout(options, Integer.parseInt(connectTimeoutString.trim()));
//		}
//		configBuilder.setFileType(opts, isBinary ? FtpFileType.BINARY : FtpFileType.ASCII);

		return options;
	}

	private FileSystemOptions getSFTPOptions(boolean isBinary) {

		String strictHostKeyChecking = getProperties().getProperty("strictHostKeyChecking", "no");
		String timeoutString = getProperties().getProperty("timeout");

		FileSystemOptions options = new FileSystemOptions();

		try {
			SftpFileSystemConfigBuilder configBuilder = SftpFileSystemConfigBuilder.getInstance();
			configBuilder.setUserDirIsRoot(options, false);

			configBuilder.setStrictHostKeyChecking(options, strictHostKeyChecking.trim());
			if (timeoutString != null) {
				configBuilder.setTimeout(options, Integer.parseInt(timeoutString.trim()));
			}
		}
		catch (FileSystemException ex) {
			throw new RuntimeException("Error configuring SFTP options: " + ex.getMessage());
		}

		return options;
	}

	private String getRemoteFileURI(String remoteFileName) {

		String protocol = getProperties().getProperty("protocol").trim();
		String hostname = percentEncode(getProperties().getProperty("hostname").trim());
		String user = percentEncode(getProperties().getProperty("user").trim());
		String password = percentEncode(getProperties().getProperty("password").trim());

		String cleanedFileName = percentEncodeExceptSlash(remoteFileName);

		return protocol + "://" + user + ":" + password +  "@" + hostname + cleanedFileName;
	}

	static Map<String,String> percentEncodings;
	static {
		percentEncodings = new HashMap<String, String>();
		percentEncodings.put(" ", "%20");
		percentEncodings.put("!", "%21");
		percentEncodings.put("#", "%23");
		percentEncodings.put("$", "%24");
		percentEncodings.put("&", "%26");
		percentEncodings.put("'", "%27");
		percentEncodings.put("(", "%28");
		percentEncodings.put(")", "%29");
		percentEncodings.put("*", "%2A");
		percentEncodings.put("+", "%2B");
		percentEncodings.put(",", "%2C");
		percentEncodings.put(":", "%3A");
		percentEncodings.put(";", "%3B");
		percentEncodings.put("=", "%3D");
		percentEncodings.put("?", "%3F");
		percentEncodings.put("@", "%40");
		percentEncodings.put("[", "%5B");
		percentEncodings.put("]", "%5D");
	}

	private String percentEncode(String raw) {
		String encoded = raw.replace("%", "%25").replace("/", "%2F");
		return percentEncodeExceptPercentAndSlash(encoded);
	}

	private String percentEncodeExceptSlash(String raw) {
		String encoded = raw.replace("%", "%25");
		return percentEncodeExceptPercentAndSlash(encoded);
	}

	private String percentEncodeExceptPercentAndSlash(String raw) {
		String encoded = raw;
		for (Map.Entry<String, String> encoding : percentEncodings.entrySet()) {
			encoded = encoded.replace(encoding.getKey(), encoding.getValue());
		}
		return encoded;
	}
}
