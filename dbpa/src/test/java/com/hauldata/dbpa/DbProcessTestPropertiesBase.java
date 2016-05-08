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

package com.hauldata.dbpa;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Properties for running process and task test cases.
 * 
 * You must provide an extension class named DbProcessTestProperties that implements the abstract methods in this class.
 * Sample code is provided with each method below.
 */
public abstract class DbProcessTestPropertiesBase {

	// Get database connection properties.

	public abstract Properties getConnectionProperties();
/*
	@Override
	public Properties getConnectionProperties() {

		// Set up database connection properties.

		Properties connProps = new Properties();
		connProps.put("driver", "com.mysql.jdbc.Driver");
		connProps.put("url", "jdbc:mysql://localhost/test");
		connProps.put("allowMultiQueries", "true");
		connProps.put("user", "YOUR_USER");
		connProps.put("password", "YOUR_PASSWORD");

		return connProps;
	}
*/
	
	// Get email session properties.

	public abstract Properties getMailProperties();
/*
	@Override
	public Properties getMailProperties() {

		// Set up email session properties.

		Properties mailProps = new Properties();
		mailProps.put("mail.smtp.starttls.enable", "true");
		mailProps.put("mail.smtp.auth", "true");
		mailProps.put("mail.smtp.host", "smtp.gmail.com");
		mailProps.put("mail.smtp.port", "587");
		mailProps.put("user", "YOUR_USER");
		mailProps.put("password", "YOUR_PASSWORD");

		return mailProps;
	}
*/
	
	// Get FTP connection properties.

	public abstract Properties getFtpProperties();
/*
	@Override
	public Properties getFtpProperties() {

		// Set up FTP connection properties.

		Properties ftpProps = new Properties();
		ftpProps.put("protocol", "sftp");
		ftpProps.put("timeout", "10000");
		ftpProps.put("hostname", "ftp.YOUR_COMPANY.com");
		ftpProps.put("user", "YOUR_USER");
		ftpProps.put("password", "YOUR_PASSWORD");

		return ftpProps;
	}
 */

	// Get path properties.

	public Properties getPathProperties() {

		final String readPath = "src/test/resources/data";
		final String writePath = "target/test/resources/data";
		final String logPath = "target/test/resources/logs";
		final String processPath = "src/test/resources/process";

		try {
			// Create as necessary all target directories to which DbProcessTest writes.

			Files.createDirectories(Paths.get(writePath));
			Files.createDirectories(Paths.get(writePath, "child"));
			Files.createDirectories(Paths.get(writePath, "trash"));
			Files.createDirectories(Paths.get(logPath));
		} catch (IOException ex) {
			throw new RuntimeException(ex.getLocalizedMessage());
		}

		Properties pathProps = new Properties();
		pathProps.put("read", readPath);
		pathProps.put("write", writePath);
		pathProps.put("log", logPath);
		pathProps.put("process", processPath);

		return pathProps;
	}
}
