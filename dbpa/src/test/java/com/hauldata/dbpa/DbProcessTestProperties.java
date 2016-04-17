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

import java.util.Properties;

/**
 * Properties for running process and task test cases.
 * 
 * You must provide an implementation class named DbProcessTestPropertiesImpl that implements this interface.
 * Sample code is provided with each method below.
 */
public interface DbProcessTestProperties {

	// Get database connection properties.

	Properties getConnectionProperties();
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

	Properties getMailProperties();
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

	Properties getFtpProperties();
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

	// Get data file path.

	String  getDataPath();

	// Get log file path.

	String  getLogPath();
}
