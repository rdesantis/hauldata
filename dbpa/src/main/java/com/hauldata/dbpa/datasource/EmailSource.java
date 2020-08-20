/*
 * Copyright (c) 2020, Ronald DeSantis
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

package com.hauldata.dbpa.datasource;

import java.io.IOException;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.mail.BodyPart;
import javax.mail.Flags;
import javax.mail.Flags.Flag;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.Store;
import javax.mail.internet.MimeBodyPart;
import javax.mail.search.AndTerm;
import javax.mail.search.BodyTerm;
import javax.mail.search.ComparisonTerm;
import javax.mail.search.FlagTerm;
import javax.mail.search.FromStringTerm;
import javax.mail.search.OrTerm;
import javax.mail.search.ReceivedDateTerm;
import javax.mail.search.SearchTerm;
import javax.mail.search.SubjectTerm;

import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.VariableType;

public class EmailSource implements Source {

	public enum Field { count, sender, received, subject, body, attachmentCount, attachmentName };
	public enum Status { unread, read, all };

	private static Map<Field, VariableType> fieldTypes = new HashMap<Field, VariableType>();
	static {
		fieldTypes.put(Field.count, VariableType.INTEGER);
		fieldTypes.put(Field.sender, VariableType.VARCHAR);
		fieldTypes.put(Field.received, VariableType.DATETIME);
		fieldTypes.put(Field.subject, VariableType.VARCHAR);
		fieldTypes.put(Field.body, VariableType.VARCHAR);
		fieldTypes.put(Field.attachmentCount, VariableType.INTEGER);
		fieldTypes.put(Field.attachmentName, VariableType.VARCHAR);
	}

	public static VariableType typeOf(Field field) {
		return fieldTypes.get(field);
	}

	private EmailConnection connection;
	private ArrayList<Field> fields;
	private Status status;
	private Expression<String> folderName;
	private List<Expression<String>> senders;
	private Expression<LocalDateTime> after;
	private Expression<LocalDateTime> before;
	private List<Expression<String>> subject;
	private List<Expression<String>> body;
	private List<Expression<String>> attachmentName;
	private boolean detach;
	private Expression<String> attachmentDirectory;
	private Boolean markReadNotUnread;
	private Expression<String> targetFolderName;
	private boolean delete;

	private List<String> attachmentNameSearchTerms;
	private String attachmentTargetDirectory;

	private Store store;
	private Folder folder;
	private Message[] messages;
	private Folder targetFolder;

	private Iterator iterator;

	public EmailSource(
			EmailConnection connection,
			ArrayList<Field> fields,
			Status status,
			Expression<String> folderName,
			List<Expression<String>> senders,
			Expression<LocalDateTime> after,
			Expression<LocalDateTime> before,
			List<Expression<String>> subject,
			List<Expression<String>> body,
			List<Expression<String>> attachmentName,
			boolean detach,
			Expression<String> attachmentDirectory,
			Boolean markReadNotUnread,
			Expression<String> targetFolderName,
			boolean delete) {
		this.connection = connection;
		this.fields = fields;
		this.status = status;
		this.folderName = folderName;
		this.senders = senders;
		this.after = after;
		this.before = before;
		this.subject = subject;
		this.body = body;
		this.attachmentName = attachmentName;
		this.detach = detach;
		this.attachmentDirectory = attachmentDirectory;
		this.markReadNotUnread = markReadNotUnread;
		this.targetFolderName = targetFolderName;
		this.delete = delete;
	}

	@Override
	public boolean hasMetadata() {
		return true;
	}

	@Override
	public void executeQuery(Context context) throws InterruptedException {

		attachmentNameSearchTerms = new LinkedList<String>();
		if (attachmentName != null) {
			for (Expression<String> expression : attachmentName) {
				attachmentNameSearchTerms.add(expression.evaluate());
			}
		}

		if (detach) {
			String directory = (attachmentDirectory != null) ? attachmentDirectory.evaluate() : ".";
			attachmentTargetDirectory = context.getReadPath(directory).toString();
		}

		store = null;
		folder = null;
		targetFolder = null;
		try {
			store = context.getSession(connection).getStore();

			store.connect();

			folder = (folderName != null) ? store.getFolder(folderName.evaluate()) : store.getDefaultFolder();
			int mode = ((markReadNotUnread == null) && (targetFolderName == null) && !delete) ? Folder.READ_ONLY : Folder.READ_WRITE;
			folder.open(mode);

			if (targetFolderName != null) {
				targetFolder = store.getFolder(targetFolderName.evaluate());
				if (!targetFolder.exists()) {
					targetFolder.create(Folder.HOLDS_MESSAGES | Folder.HOLDS_FOLDERS);
				}
				targetFolder.open(Folder.READ_WRITE);
			}

			SearchTerm searchTerm = buildSearchTerm();
			messages = (searchTerm != null) ? folder.search(searchTerm) : folder.getMessages();

			iterator = makeIterator();
		}
		catch (MessagingException ex) {
			throw new RuntimeException(ex.toString());
		}
	}

	private SearchTerm buildSearchTerm() {
		ArrayList<SearchTerm> terms = new ArrayList<SearchTerm>();

		if (status != Status.all) {
			terms.add(getStatusTerm());
		}
		if (senders != null) {
			terms.add(getSendersTerm());
		}
		if (after != null) {
			terms.add(getAfterTerm());
		}
		if (before != null) {
			terms.add(getBeforeTerm());
		}
		if (subject != null) {
			terms.add(getSubjectTerm());
		}
		if (body != null) {
			terms.add(getBodyTerm());
		}

		return
				terms.isEmpty() ? null :
				(terms.size() == 1) ? terms.get(0) :
				new AndTerm(terms.toArray(new SearchTerm[terms.size()]));
	}

	private SearchTerm getStatusTerm() {
		return new FlagTerm(new Flags(Flag.SEEN), status.equals(Status.read));
	}

	private SearchTerm getSendersTerm() {
		ArrayList<SearchTerm> terms = new ArrayList<SearchTerm>();
		for (Expression<String> expression : senders) {
			String sendersString = expression.evaluate();
			for (String sender : splitSenders(sendersString)) {
				terms.add(new FromStringTerm(sender));
			}
		}
		return
				(terms.size() == 1) ? terms.get(0) :
				new OrTerm(terms.toArray(new SearchTerm[terms.size()]));
	}

	private List<String> splitSenders(String sendersString) {
		if (hasOddNumberOfQuotes(sendersString)) {
			throw new RuntimeException("SENDER has mismatched quotes");
		}

		// Assume an address may include a quoted portion which may contain commas.
		// Reassemble senders that were erroneously split at commas inside quotes.
		// If a part has an odd number of quotes, it is part of an erroneous split.
		// Merge it into the next part until a part contains an even number of quotes.

		List<String> result = new LinkedList<String>();

		String[] parts = sendersString.split(",");
		for (int i = 0; i < parts.length; ++i) {
			if (hasOddNumberOfQuotes(parts[i])) {
				parts[i + 1] = parts[i] + "," + parts[i + 1];
			}
			else {
				result.add(parts[i].trim());
			}
		}

		return result;
	}

	private boolean hasOddNumberOfQuotes(String string) {
		boolean countIsOdd = false;
		for (int i = 0; i < string.length(); ++i) {
			if (string.charAt(i) == '"') {
				countIsOdd = !countIsOdd;
			}
		}
		return countIsOdd;
	}

	private SearchTerm getAfterTerm() {
		return new ReceivedDateTerm(ComparisonTerm.GE, Date.from(after.evaluate().atZone(ZoneId.systemDefault()).toInstant()));
	}

	private SearchTerm getBeforeTerm() {
		return new ReceivedDateTerm(ComparisonTerm.LT, Date.from(before.evaluate().atZone(ZoneId.systemDefault()).toInstant()));
	}

	private SearchTerm getSubjectTerm() {
		return getContainsTerm(subject, s -> new SubjectTerm(s));
	}

	private SearchTerm getBodyTerm() {
		return getContainsTerm(body, s -> new BodyTerm(s));
	}

	private SearchTerm getContainsTerm(List<Expression<String>> expressions, Function<String, SearchTerm> newTerm) {

		ArrayList<SearchTerm> terms = new ArrayList<SearchTerm>();
		for (Expression<String> expression : expressions) {
			terms.add(newTerm.apply(expression.evaluate()));
		}
		return
				(terms.size() == 1) ? terms.get(0) :
				new AndTerm(terms.toArray(new SearchTerm[terms.size()]));
	}

	private Iterator makeIterator() {
		Iterator iterator;
		if (fields.contains(Field.attachmentName)) {
			iterator = new AttachmentNameIterator();
		}
		else if (attachmentName != null) {
			iterator = new AttachmentFilterIterator();
		}
		else if (fields.contains(Field.attachmentCount) || detach) {
			iterator = new AttachmentProcessingIterator();
		}
		else {
			iterator = new SimpleIterator();
		}

		return (fields.contains(Field.count)) ? new CountIterator(iterator) : iterator;
	}

	@Override
	public int getColumnCount() {
		return fields.size();
	}

	@Override
	public String getColumnLabel(int column) {
		return fields.get(column - 1).name();
	}

	@Override
	public boolean next() throws InterruptedException {
		return iterator.next();
	}

	private interface Iterator {
		Message getMessage();

		boolean next();
		boolean isLast();
	}

	private class SimpleIterator implements Iterator {

		protected int messageIndex;
		protected Message message;

		protected SimpleIterator() {
			messageIndex = -1;
		}

		protected boolean isMessageActionable() {
			return true;
		}

		@Override
		public Message getMessage() {
			return message;
		}

		@Override
		public boolean next() {
			if ((0 <= messageIndex) && isMessageActionable()) {
				actOnMessage();
			}

			boolean hasNext = (++messageIndex < messages.length);
			if (hasNext) {
				message = messages[messageIndex];
				processAttachments();
			}

			return hasNext;
		}

		protected void processAttachments() {}

		@Override
		public boolean isLast() {
			return (messageIndex == (messages.length - 1));
		}
	}

	private interface AttachmentCountingIterator {
		int getAttachmentCount();
	}

	class AttachmentProcessingIterator extends SimpleIterator implements AttachmentCountingIterator {
		private ArrayList<String> attachmentNames;

		public ArrayList<String> getAttachmentNames() { return attachmentNames; }

		@Override
		public int getAttachmentCount() { return attachmentNames.size(); }

		@Override
		protected void processAttachments() {
			attachmentNames = new ArrayList<String>();
			try {
				Object content = message.getContent();
				if (content instanceof Multipart) {
					Multipart multipart = (Multipart)content;
					for (int i = 0; i < multipart.getCount(); ++i) {
						BodyPart bodyPart = multipart.getBodyPart(i);
						if (
								bodyPart instanceof MimeBodyPart &&
								Part.ATTACHMENT.equalsIgnoreCase(bodyPart.getDisposition()) &&
								attachmentNameMatch(bodyPart.getFileName())) {

							attachmentNames.add(bodyPart.getFileName());

							if (detach) {
								String filePath = attachmentTargetDirectory + "/" + bodyPart.getFileName();
								((MimeBodyPart)bodyPart).saveFile(filePath);
							}
						}
					}
				}
			}
			catch (IOException | MessagingException ex) {
				throw new RuntimeException("Error processing attachment: " + ex.toString());
			}
		}

		private boolean attachmentNameMatch(String fileName) {
			for (String term : attachmentNameSearchTerms) {
				if (!fileName.contains(term)) {
					return false;
				}
			}
			return true;
		}
	}

	class AttachmentFilterIterator implements Iterator, AttachmentCountingIterator {
		private AttachmentProcessingIterator iterator;
		private boolean last;
		private Message message;
		private int attachmentCount;
		private ArrayList<String> attachmentNames;

		public AttachmentFilterIterator() {
			iterator = new AttachmentProcessingIterator() {
				@Override
				protected boolean isMessageActionable() {
					return (0 < getAttachmentCount());
				}
			};

			last = !lookAhead();
			attachmentCount = 0;
		}

		private boolean lookAhead() {
			while (iterator.next()) {
				if (iterator.isMessageActionable()) {
					return true;
				}
			}
			return false;
		}

		@Override
		public Message getMessage() { return message; }

		@Override
		public int getAttachmentCount() { return attachmentCount; }

		public ArrayList<String> getAttachmentNames() { return attachmentNames; }

		@Override
		public boolean next() {
			if (last) {
				return false;
			}
			else {
				message = iterator.getMessage();
				attachmentCount = iterator.getAttachmentCount();
				attachmentNames = iterator.getAttachmentNames();

				last = !lookAhead();

				return true;
			}
		}

		@Override
		public boolean isLast() { return last; }
	}

	class AttachmentNameIterator implements Iterator, AttachmentCountingIterator {
		private AttachmentFilterIterator iterator;
		private int attachmentIndex;

		public AttachmentNameIterator() {
			iterator = new AttachmentFilterIterator();
			attachmentIndex = -1;
		}

		public String getAttachmentName() {
			return iterator.getAttachmentNames().get(attachmentIndex);
		}

		@Override
		public int getAttachmentCount() {
			return iterator.getAttachmentCount();
		}

		@Override
		public Message getMessage() {
			return iterator.getMessage();
		}

		@Override
		public boolean next() {
			if (++attachmentIndex < iterator.getAttachmentCount()) {
				return true;
			}
			else if (!iterator.next()) {
				return false;
			}
			else {
				attachmentIndex = 0;
				return true;
			}
		}

		@Override
		public boolean isLast() {
			return iterator.isLast() && (attachmentIndex == (iterator.getAttachmentCount() - 1));
		}
	}

	private class CountIterator implements Iterator {
		private int count;
		private boolean first;

		public CountIterator(Iterator resultIterator) {
			count = 0;
			while (resultIterator.next()) {
				++count;
			}
			first = true;
		}

		@Override
		public Message getMessage() {
			return null;
		}

		public int getCount() {
			return count;
		}

		@Override
		public boolean next() {
			boolean result = first;
			first = false;
			return result;
		}

		@Override
		public boolean isLast() {
			return true;
		}
	}

	private void actOnMessage() {
		if (markReadNotUnread != null) {
			markRead(markReadNotUnread);
		}
		if (targetFolder != null) {
			move();
		}
		if (delete) {
			delete();
		}
	}

	private void markRead(boolean status) {
		try {
			iterator.getMessage().setFlag(Flags.Flag.SEEN, status);
		}
		catch (MessagingException ex) {
			throw new RuntimeException("Error marking message READ or UNREAD: " + ex.toString());
		}
	}

	private void move() {
		try {
			folder.copyMessages(new Message[] { iterator.getMessage() }, targetFolder);
		}
		catch (MessagingException ex) {
			throw new RuntimeException("Error copying message to target folder: " + ex.toString());
		}
		delete();
	}

	private void delete() {
		try {
			iterator.getMessage().setFlag(Flags.Flag.DELETED, true);
			folder.expunge();
		}
		catch (MessagingException ex) {
			throw new RuntimeException("Error deleting message from source folder: " + ex.toString());
		}
	}

	@Override
	public Object getObject(int columnIndex) {
		try {
			switch (fields.get(columnIndex - 1)) {
			case count:
				return ((CountIterator)iterator).getCount();
			case sender:
				return iterator.getMessage().getFrom()[0].toString();
			case received:
				return iterator.getMessage().getReceivedDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
			case subject:
				return iterator.getMessage().getSubject();
			case body:
				return getText(iterator.getMessage());
			case attachmentCount:
				return ((AttachmentCountingIterator)iterator).getAttachmentCount();
			case attachmentName:
				return ((AttachmentNameIterator)iterator).getAttachmentName();
			default:
				throw new RuntimeException("Internal error: EmailSource.getObject(int) unhandled field");
			}
		}
		catch (MessagingException | IOException ex) {
			throw new RuntimeException("Error retrieving field #" + String.valueOf(columnIndex) + " of type " +  fields.get(columnIndex).name() + ": " + ex.toString());
		}
	}

	/**
     * Return the primary text content of the message.
     * Copied directly from https://javaee.github.io/javamail/FAQ#mainbody
     */
	private String getText(Part p) throws MessagingException, IOException {
		if (p.isMimeType("text/*")) {
			String s = (String)p.getContent();
			return s;
		}

		if (p.isMimeType("multipart/alternative")) {
			// prefer html text over plain text
			Multipart mp = (Multipart)p.getContent();
			String text = null;
			for (int i = 0; i < mp.getCount(); i++) {
				Part bp = mp.getBodyPart(i);
				if (bp.isMimeType("text/plain")) {
					if (text == null)
						text = getText(bp);
					continue;
				}
				else if (bp.isMimeType("text/html")) {
					String s = getText(bp);
					if (s != null)
					return s;
				}
				else {
					return getText(bp);
				}
			}
			return text;
		}
		else if (p.isMimeType("multipart/*")) {
			Multipart mp = (Multipart)p.getContent();
			for (int i = 0; i < mp.getCount(); i++) {
				String s = getText(mp.getBodyPart(i));
				if (s != null)
					return s;
			}
		}
		return null;
	}

	@Override
	public boolean isLast() {
		return iterator.isLast();
	}

	@Override
	public void done(Context context) {}

	@Override
	public void close(Context context) {
		if (targetFolder != null) {
			try { targetFolder.close(false); } catch (Exception ex) {}
		}
		if (folder != null) {
			try { folder.close(false); } catch (Exception ex) {}
		}
		if ((store != null) && store.isConnected()) {
			try { store.close(); } catch (Exception ex) {}
		}
	}
}
