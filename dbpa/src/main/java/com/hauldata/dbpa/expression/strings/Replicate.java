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

package com.hauldata.dbpa.expression.strings;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.variable.VariableType;

public class Replicate extends Expression<String> {

	private Expression<String> string;
	private Expression<Integer> repeats;

	public Replicate(Expression<String> string, Expression<Integer> repeats) {
		super(VariableType.VARCHAR);
		this.string = string;
		this.repeats = repeats;
	}

	@Override
	public String evaluate() {
		String stringValue = string.evaluate();
		Integer repeatsValue = repeats.evaluate();

		if (stringValue == null || repeatsValue == null || repeatsValue < 0) {
			return null;
		}

		StringBuilder result = new StringBuilder();
		for (int i = 0; i < repeatsValue; i++) {
			result.append(stringValue);
		}

		return result.toString();
	}
}
