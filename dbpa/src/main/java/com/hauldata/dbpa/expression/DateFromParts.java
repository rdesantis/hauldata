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

import java.time.LocalDate;
import java.time.LocalDateTime;

import com.hauldata.dbpa.variable.VariableType;

public class DateFromParts extends Expression<LocalDateTime> {

	public DateFromParts(Expression<Integer> year, Expression<Integer> month, Expression<Integer> day) {
		super(VariableType.DATETIME);
		this.year = year;
		this.month = month;
		this.day = day;
	}

	@Override
	public LocalDateTime evaluate() {
		Integer yearValue = year.evaluate();
		Integer monthValue = month.evaluate();
		Integer dayValue = day.evaluate();
		
		if (yearValue == null || monthValue == null || dayValue == null) {
			return null;
		}

		return LocalDate.of(yearValue, monthValue, dayValue).atStartOfDay();
	}

	private Expression<Integer> year;
	private Expression<Integer> month;
	private Expression<Integer> day;
}
