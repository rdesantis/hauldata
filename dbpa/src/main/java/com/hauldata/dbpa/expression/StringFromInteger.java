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

import com.hauldata.dbpa.variable.VariableType;

public class StringFromInteger extends Expression<String> {

	public StringFromInteger(Expression<Integer> term, Expression<String> format) {
		super(VariableType.VARCHAR);
		this.term = term;
		this.format = format;
	}

	@Override
	public String evaluate() {
		Integer value = term.evaluate();
		String format = this.format.evaluate();

		if (value == null || format == null) {
			return null;
		}

		// TODO: Format the value according to the format string throwing an exception if the string is not valid
//		String format = format.evaluate();
		return value.toString();
	}

	private Expression<Integer> term;
	private Expression<String> format;
}
