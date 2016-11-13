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

package com.hauldata.dbpa.log;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Pattern;

/**
 * Log appender that writes to an in-memory log with convenient access methods
 * for analyzing log content.
 */
public class Analyzer implements Appender {

	public static class Record {

		public String processId;
		public String taskId;
		public LocalDateTime datetime;
		public int level;
		public String message;
		
		Record(String processId, String taskId, LocalDateTime datetime, int level, String message) {
			this.processId = processId;
			this.taskId = taskId;
			this.datetime = datetime;
			this.level = level;
			this.message = message;
		}
	}

	private List<Record> records;

	public Analyzer() {
		records = new LinkedList<Record>();
	}

	// Appender overrides.

	@Override
	public void log(String processId, String taskId, LocalDateTime datetime, int level, String message) {
		synchronized (records) {
			records.add(new Record(processId, taskId, datetime, level, message));
		}
	}

	@Override
	public void close() {
	}

	// Iterators for accessing log records.

	/**
	 * Get iterator to access all records
	 */
	public RecordIterator recordIterator() {
		return new RecordIterator();
	}

	public class RecordIterator {
		private ListIterator<Record> iterator;

		RecordIterator() {
			iterator = records.listIterator();
		}

		public Record next() {
			return iterator.next();
		}

		public boolean hasNext() {
			return iterator.hasNext();
		}

		protected void previous() {
			iterator.previous();
		}
	}

	/**
	 * Get iterator to access only records where the processId matches a pattern
	 */
	public ProcessRecordIterator recordIterator(Pattern processIdPattern) {
		return new ProcessRecordIterator(processIdPattern);
	}

	/**
	 * Get iterator to access only records where the processId matches a literal string
	 */
	public ProcessRecordIterator recordIterator(String processId) {
		return new ProcessRecordIterator(processId);
	}

	public class ProcessRecordIterator extends RecordIterator {
		private Pattern pattern;

		ProcessRecordIterator(Pattern pattern) {
			this.pattern = pattern;
		}

		ProcessRecordIterator(String processId) {
			pattern = Pattern.compile(Pattern.quote(processId));
		}

		public Record next() {
			Record nextRecord;
			do { nextRecord = super.next();
			} while (!pattern.matcher(nextRecord.processId).matches());
			return nextRecord;
		}

		public boolean hasNext() {
			while (super.hasNext()) {
				if (pattern.matcher(super.next().processId).matches()) {
					previous();
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * Get iterator to access only records where the taskId matches a pattern
	 */
	public TaskRecordIterator recordIterator(String processId, Pattern taskIdPattern) {
		return new TaskRecordIterator(processId, taskIdPattern);
	}

	/**
	 * Get iterator to access only records where the taskId matches a literal string
	 */
	public TaskRecordIterator recordIterator(String processId, String taskId) {
		return new TaskRecordIterator(processId, taskId);
	}

	public class TaskRecordIterator extends ProcessRecordIterator {
		private Pattern pattern;

		TaskRecordIterator(String processId, Pattern taskIdPattern) {
			super(processId);
			this.pattern = taskIdPattern;
		}

		TaskRecordIterator(String processId, String taskId) {
			super(processId);
			this.pattern = Pattern.compile(Pattern.quote(taskId));
		}

		public Record next() {
			Record nextRecord;
			do { nextRecord = super.next();
			} while (!pattern.matcher(nextRecord.taskId).matches());
			return nextRecord;
		}

		public boolean hasNext() {
			while (super.hasNext()) {
				if (pattern.matcher(super.next().taskId).matches()) {
					previous();
					return true;
				}
			}
			return false;
		}
	}
}
