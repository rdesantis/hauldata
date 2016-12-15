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

public class Substring extends Expression<String> {

	private Expression<String> string;
	private Expression<Integer> start;
	private Expression<Integer> length;

	public Substring(
			Expression<String> string,
			Expression<Integer> start,
			Expression<Integer> length) {
		super(VariableType.VARCHAR);
		this.string = string;
		this.start = start;
		this.length = length;
	}

	@Override
	public String evaluate() {

		String stringValue = string.evaluate();
		Integer startValue = start.evaluate();
		Integer lengthValue = length.evaluate();

		if (stringValue == null || startValue == null || lengthValue == null) {
			return null;
		}

		if (lengthValue < 0) {
			throw new RuntimeException("Negative length for SUBSTRING");
		}

		int startIndex = Math.min(Math.max(1, startValue), stringValue.length() + 1) - 1; 
		int endIndex = Math.min(Math.max(startValue + lengthValue - 1, 0), stringValue.length());

		return stringValue.substring(startIndex, endIndex);
	}
}
