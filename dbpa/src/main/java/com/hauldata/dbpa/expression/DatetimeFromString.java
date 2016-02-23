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
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import com.hauldata.dbpa.variable.VariableType;

public class DatetimeFromString extends Expression<LocalDateTime> {

	public DatetimeFromString(Expression<String> expression) {
		super(VariableType.DATETIME);
		this.expression = expression;
	}

	@Override
	public LocalDateTime evaluate() {
		String image = expression.evaluate();

		if (image == null) {
			return null;
		}

		final DateTimeFormatter[] formatters = {
				DateTimeFormatter.ofPattern("M/d/yyyy[ h:m[:s] a]"),
				DateTimeFormatter.ofPattern("M/d/yyyy[ H:m[:s]]"),
				DateTimeFormatter.ofPattern("yyyy-M-d[ H:m[:s]]"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]"),
				};

		LocalDateTime result = null;
		for (DateTimeFormatter formatter : formatters) {
			try {
				result = LocalDateTime.parse(image, formatter);
				break;
			}
			catch (DateTimeParseException ex) {
			try {
				result = LocalDate.parse(image, formatter).atStartOfDay();
				break;
			}
			catch (DateTimeParseException exx) {}
			}
		}
		if (result == null) {
			throw new RuntimeException("Invalid date format: " + image);
		}
		
		return result;
	}

	private Expression<String> expression;
}
