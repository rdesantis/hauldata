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

import java.util.LinkedList;

/**
 * Log for process status messages
 * 
 * Concrete subclass constructors must pass processId string to Log constructor
 */
public class RootLog extends LogBase {

	public RootLog(String processId) {

		super(processId, null, new LinkedList<Logger>());
	}

	public void add(Logger logger) {
		addLogger(logger);
	}

	@Override
	public void close() {
		closeLoggers();
	}
}
