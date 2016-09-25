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

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;

public class EmailConnection extends Connection {

	private Session session = null;

	@Override
	public void setProperties(Properties properties) {
		session = null;
		super.setProperties(properties);
	}

	/**
	 * Return email server session, setting it up if required
	 * using properties specified on the constructor.
	 * @return the email session or null if session properties were not
	 * provided on the constructor.  Throws an exception if the
	 * session cannot be established.
	 */
	public Session get() {

		synchronized (this) {
			if (session == null && getProperties() != null) {
	
				String user = getProperties().getProperty("user");
				String password = getProperties().getProperty("password");
				
				if (user == null || password == null) {
					throw new RuntimeException("Required email credential properties are not set");
				}

				try {
					session = Session.getInstance(getProperties(), new PasswordAuthenticator(user, password));
				}
				catch (Exception ex) {
					throw new RuntimeException("Email session setup failed: " + ex.toString(), ex);
				}
			}
		}
		return session;
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
}
