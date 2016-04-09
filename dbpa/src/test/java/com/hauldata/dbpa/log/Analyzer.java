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

	@Override
	public void log(String processId, String taskId, LocalDateTime datetime, int level, String message) {
		records.add(new Record(processId, taskId, datetime, level, message));
	}

	@Override
	public void close() {
	}

	public List<Record> getRecords() {
		return records;
	}
}
