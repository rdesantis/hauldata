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
import java.time.temporal.ChronoUnit;

import com.hauldata.dbpa.variable.VariableType;

public class DatetimeAdd extends Expression<LocalDateTime> {

	public DatetimeAdd(Expression<LocalDateTime> datetime, ChronoUnit unit, Expression<Integer> increment) {
		super(VariableType.DATETIME);
		this.datetime = datetime;
		this.unit = unit;
		this.increment = increment;
	}
	
	@Override
	public LocalDateTime evaluate() {
		LocalDateTime original = datetime.evaluate();
		Integer delta = increment.evaluate();

		if (original == null || delta == null) {
			return null;
		}

		return original.plus(delta, unit);
	}

	private Expression<LocalDateTime> datetime;
	private ChronoUnit unit;
	private Expression<Integer> increment;
}
