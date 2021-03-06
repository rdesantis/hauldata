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
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Alert;
import com.hauldata.dbpa.process.Context;

public class EmailTask extends Task {

	private EmailConnection connection;
	private Expression<String> from;
	private List<Expression<String>> to;
	private List<Expression<String>> cc;
	private List<Expression<String>> bcc;
	private Expression<String> subject;
	private Expression<String> body;
	private boolean isHtml;
	private List<Expression<String>> attachments;

	public EmailTask(
			Prologue prologue,
			EmailConnection connection,
			Expression<String> from,
			List<Expression<String>> to,
			List<Expression<String>> cc,
			List<Expression<String>> bcc,
			Expression<String> subject,
			Expression<String> body,
			boolean isHtml,
			List<Expression<String>> attachments) {

		super(prologue);
		this.connection = connection;
		this.from = from;
		this.to = to;
		this.cc = cc;
		this.bcc = bcc;
		this.subject = subject;
		this.body = body;
		this.isHtml = isHtml;
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
				Alert.addRecipients(message, Message.RecipientType.TO, to.evaluate());
			}
			for (Expression<String> cc : this.cc) {
				Alert.addRecipients(message, Message.RecipientType.CC, cc.evaluate());
			}
			for (Expression<String> bcc : this.bcc) {
				Alert.addRecipients(message, Message.RecipientType.BCC, bcc.evaluate());
			}

			if (subject != null) {
				message.setSubject(subject.evaluate());
			}
			
			Multipart multipart = new MimeMultipart();

			if (body != null) {
				BodyPart messageBodyPart = new MimeBodyPart();
				messageBodyPart.setContent(body.evaluate(), "text/" + (isHtml ? "html" : "plain"));

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
}
