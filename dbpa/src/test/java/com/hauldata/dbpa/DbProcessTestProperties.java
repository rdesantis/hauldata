package com.hauldata.dbpa;

import java.util.Properties;

public class DbProcessTestProperties extends DbProcessTestPropertiesBase {

	@Override
	public Properties getConnectionProperties() {

		// Set up database connection properties.

		Properties connProps = new Properties();
		connProps.put("driver", "com.mysql.jdbc.Driver");
		connProps.put("url", "jdbc:mysql://localhost/test");
		connProps.put("allowMultiQueries", "true");
		connProps.put("user", "insecure");
		connProps.put("password", "password");

		return connProps;
	}

	@Override
	public Properties getMailProperties() {

		// Set up email session properties.

		Properties mailProps = new Properties();
		mailProps.put("mail.smtp.starttls.enable", "true");
		mailProps.put("mail.smtp.auth", "true");
		mailProps.put("mail.smtp.host", "smtp.gmail.com");
		mailProps.put("mail.smtp.port", "587");
		mailProps.put("user", "rdesantis@cmtnyc.com");
		mailProps.put("password", "Welcome2CMT");

		return mailProps;
	}

	@Override
	public Properties getFtpProperties() {

		// Set up FTP connection properties.

		Properties ftpProps = new Properties();
		ftpProps.put("protocol", "sftp");
		ftpProps.put("timeout", "10000");
		ftpProps.put("hostname", "ftp.creativemobiletechnologies.com");
		ftpProps.put("user", "rdesantis");
		ftpProps.put("password", "LPa8gy");

		return ftpProps;
	}
}
