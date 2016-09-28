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

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.List;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.util.tokenizer.Delimiter;
import com.hauldata.util.tokenizer.DsvTokenizer;
import com.hauldata.util.tokenizer.Token;

public class EmailTask extends Task {

	private EmailConnection connection;
	private Expression<String> from;
	private List<Expression<String>> to;
	private List<Expression<String>> cc;
	private Expression<String> subject;
	private Expression<String> body;
	private List<Expression<String>> attachments;

	public EmailTask(
			Prologue prologue,
			EmailConnection connection,
			Expression<String> from,
			List<Expression<String>> to,
			List<Expression<String>> cc,
			Expression<String> subject,
			Expression<String> body,
			List<Expression<String>> attachments) {

		super(prologue);
		this.connection = connection;
		this.from = from;
		this.to = to;
		this.cc = cc;
		this.subject = subject;
		this.body = body;
		this.attachments = attachments;
	}

	@Override
	protected void execute(Context context) {

		// See http://stackoverflow.com/questions/3649014/send-email-using-java for basic mail
		// See http://www.tutorialspoint.com/javamail_api/javamail_api_send_email_with_attachment.htm for attachments

		try {
			Message message = new MimeMessage(context.getSession(connection));

			message.setFrom(new InternetAddress(from.evaluate()));
			for (Expression<String> to : this.to) {
				addRecipients(message, Message.RecipientType.TO, to);
			}
			for (Expression<String> cc : this.cc) {
				addRecipients(message, Message.RecipientType.CC, cc);
			}

			if (subject != null) {
				message.setSubject(subject.evaluate());
			}
			
			Multipart multipart = new MimeMultipart();

			if (body != null) {
				BodyPart messageBodyPart = new MimeBodyPart();
				messageBodyPart.setText(body.evaluate());
				
				multipart.addBodyPart(messageBodyPart);
			}

			for (Expression<String> attachment : attachments) {
				Path path = context.getWritePath(attachment.evaluate());
				context.files.assureNotOpen(path);

				BodyPart attachmentBodyPart = new MimeBodyPart();
				DataSource source = new FileDataSource(path.toString());
				attachmentBodyPart.setDataHandler(new DataHandler(source));
				attachmentBodyPart.setFileName(path.getFileName().toString());

				multipart.addBodyPart(attachmentBodyPart);
			}

			if (multipart.getCount() > 0) {
				message.setContent(multipart);
			}
			else {
				message.setText("");
			}

			Transport.send(message);
		}
		catch (MessagingException | IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Email messaging failed: " + message);
		}
	}

	private void addRecipients(Message message, Message.RecipientType type, Expression<String> recipients)
			throws AddressException, IOException, MessagingException {

		final Delimiter comma = new Delimiter(false, ",");

		String evaluatedRecipients = recipients.evaluate() + ",";
		DsvTokenizer tokenizer = new DsvTokenizer(new StringReader(evaluatedRecipients), ',');

		StringBuilder recipient = new StringBuilder();
		while (tokenizer.hasNext()) {
			Token nextToken = tokenizer.nextToken();
			if (nextToken.equals(comma)) {
				String address = recipient.toString().trim();
				if (!address.isEmpty()) {
					message.addRecipient(type, new InternetAddress(address));
				}
				recipient = new StringBuilder();
			}
			else {
				recipient.append(nextToken.render());
			}
		}
	}
}
