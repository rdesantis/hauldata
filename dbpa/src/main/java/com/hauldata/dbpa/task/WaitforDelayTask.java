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

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

import com.hauldata.dbpa.expression.Expression;

public class WaitforDelayTask extends WaitforTask {

	public WaitforDelayTask(
			Prologue prologue,
			Expression<String> delay) {
		super(prologue, delay);
	}

	protected long sleepMillis(String delay) {

		final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("H:m[:s]");

		LocalTime pseudoTime;
		try {
			pseudoTime = LocalTime.parse(delay, formatter);
		}
		catch (DateTimeParseException ex) {
			throw new RuntimeException("Invalid delay expression: " + delay);
		}

		return LocalTime.MIN.until(pseudoTime, ChronoUnit.MILLIS);
	}
}
