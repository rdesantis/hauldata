/*
 * Copyright (c) 2017, Ronald DeSantis
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

import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.process.ContextProperties;

class RunDbpAlert {

	public static void send(String processID, ContextProperties contextProps, String failMessage) {

		Properties properties = contextProps.getProperties("mail");
		String from = properties.getProperty("alertFrom");
		String to = properties.getProperty("alertTo");
		String subject = properties.getProperty("alertSubject", "DBPA script failure: %s");
		
		if (from == null || to == null) {
			throw new RuntimeException("Email alertFrom and/or alertTo properties are not set");
		}

		EmailConnection connection = new EmailConnection();
		connection.setProperties(properties);

		Session session = connection.get();
		Message message = new MimeMessage(session);
		try {
			message.setFrom(new InternetAddress(from));
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
			message.setSubject(String.format(subject, processID));
			Multipart multipart = new MimeMultipart();
			BodyPart messageBodyPart = new MimeBodyPart();
			messageBodyPart.setText(failMessage);
			multipart.addBodyPart(messageBodyPart);
			message.setContent(multipart);

			Transport.send(message);
		}
		catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}
}
