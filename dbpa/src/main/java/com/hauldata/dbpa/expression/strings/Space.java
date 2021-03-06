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

public class Space extends Expression<String> {

	private Expression<Integer> repeats;

	public Space(Expression<Integer> repeats) {
		super(VariableType.VARCHAR);
		this.repeats = repeats;
	}

	@Override
	public String evaluate() {
		Integer repeatsValue = repeats.evaluate();

		if (repeatsValue == null || repeatsValue < 0) {
			return null;
		}
		else if (repeatsValue == 0) {
			return "";
		}

		String formatter = String.format("%%%ds", repeatsValue);
		return String.format(formatter, "");
	}
}
