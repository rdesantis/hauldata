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

public class Replace extends Expression<String> {

	private Expression<String> string;
	private Expression<String> pattern;
	private Expression<String> replacement;

	public Replace(
			Expression<String> string,
			Expression<String> pattern,
			Expression<String> replacement) {
		super(VariableType.VARCHAR);
		this.string = string;
		this.pattern = pattern;
		this.replacement = replacement;
	}

	@Override
	public String evaluate() {
		String stringValue = string.evaluate();
		String patternValue = pattern.evaluate();
		String replacementValue = replacement.evaluate();

		if (stringValue == null || patternValue == null || replacementValue == null) {
			return null;
		}

		if (patternValue.isEmpty()) {
			throw new RuntimeException("Empty string used for REPLACE pattern");
		}

		return stringValue.replace(patternValue, replacementValue);
	}
}
