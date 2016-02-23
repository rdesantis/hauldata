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
import java.time.format.DateTimeFormatter;

import com.hauldata.dbpa.variable.VariableType;

public class StringFromDatetime extends Expression<String> {

	public StringFromDatetime(Expression<LocalDateTime> expression, Expression<String> formatExpression) {
		super(VariableType.VARCHAR);
		this.expression = expression;
		this.formatExpression = formatExpression;
	}

	@Override
	public String evaluate() {
		LocalDateTime value = expression.evaluate();
		String pattern = formatExpression.evaluate();

		if (value == null || pattern == null) {
			return null;
		}

		return value.format(DateTimeFormatter.ofPattern(pattern));
	}

	private Expression<LocalDateTime> expression;
	private Expression<String> formatExpression;
}
