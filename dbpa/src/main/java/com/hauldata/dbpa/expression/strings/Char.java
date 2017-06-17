/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.dbpa.expression.strings;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.variable.VariableType;

public class Char extends Expression<String> {

	private Expression<Integer> ascii;

	public Char(Expression<Integer> ascii) {
		super(VariableType.VARCHAR);
		this.ascii = ascii;
	}

	@Override
	public String evaluate() {
		Integer asciiValue = ascii.evaluate();

		if (asciiValue == null || asciiValue < 0 || 255 < asciiValue) {
			return null;
		}

		return String.valueOf((char)asciiValue.intValue());
	}
}
