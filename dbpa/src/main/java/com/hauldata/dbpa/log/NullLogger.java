/*
 * Copyright (c) 2016, 2018, Ronald DeSantis
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

public class NullLogger implements Logger {
	
	public static NullLogger logger = new NullLogger();

	private NullLogger() {}

	@Override
	public void addAppender(Appender appender) {}

	@Override
	public void info(String taskId, String message) {}

	@Override
	public void warn(String taskId, String message) {}

	@Override
	public void error(String taskId, String message) {}
		
	@Override
	public void message(String taskId, String message) {}

	@Override
	public void error(String taskId, Exception exception) {}

	@Override
	public Logger nestTask(String nestedTaskId) { return this; }

	@Override
	public Logger nestProcess(String taskId, String nestedProcessId) { return this; }

	@Override
	public void close() {}
}
