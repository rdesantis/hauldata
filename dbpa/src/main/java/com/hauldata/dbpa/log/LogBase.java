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
import java.util.List;

public abstract class LogBase implements Log {
	
	protected LogBase(String processId, String taskPrefix, List<Logger> loggers) {
		this.processId = processId;
		this.taskPrefix = taskPrefix;
		this.loggers = loggers;
	}

	protected void addLogger(Logger logger) {
		loggers.add(logger);
	}

	@Override
	public void write(String taskId, String message) {

		String resolvedProcessId = (processId == null) ? "" : processId;
		String resolvedTaskId = (taskPrefix == null) ? taskId : taskPrefix + "." + taskId;

		for (Logger logger : loggers) {
			logger.log(resolvedProcessId, resolvedTaskId, LocalDateTime.now(), message);
		}
	}

	@Override
	public Log nestTask(String nestedTaskId) {

		String resolvedTaskId = (taskPrefix == null) ? nestedTaskId : taskPrefix + "." + nestedTaskId;
		return new NestedLog(processId, resolvedTaskId, loggers);
	}

	@Override
	public Log nestProcess(String nestedProcessId) {

		String resolvedProcessId = (processId == null) ? nestedProcessId : processId + "." + nestedProcessId;
		return new NestedLog(resolvedProcessId, null, loggers);
	}

	protected void closeLoggers() {
		
		for (Logger logger : loggers) {
			logger.close();
		}
	}

	@Override
	public abstract void close();

	private String processId;
	private String taskPrefix;
	private List<Logger> loggers;
}
