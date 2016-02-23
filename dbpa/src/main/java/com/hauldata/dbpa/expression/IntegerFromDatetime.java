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

package com.hauldata.dbpa.expression;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;

import com.hauldata.dbpa.variable.VariableType;

public class IntegerFromDatetime extends Expression<Integer> {
	
	/**
	 * Instantiate a function call that extracts a component from a datetime
	 * and returns it as an integer
	 */
	public IntegerFromDatetime(Expression<LocalDateTime> datetime, ChronoField field) {
		super(VariableType.INTEGER);
		this.datetime = datetime;
		this.field = field;
	}
	
	@Override
	public Integer evaluate() {
		LocalDateTime argument = datetime.evaluate();

		if (argument == null) {
			return null;
		}

		int result = argument.get(field);
		if (field == ChronoField.DAY_OF_WEEK) {
			// ChronoField.DAY_OF_WEEK uses MONDAY = 1 through SUNDAY = 7
			// Transact SQL DATEPART(WEEKDAY, date) uses SUNDAY = 1 through SATURDAY = 7 by default (but is configurable) 
			if (++result == 8) { result = 1; } 
		}
		return result;
	}

	private Expression<LocalDateTime> datetime;
	private ChronoField field;
}
