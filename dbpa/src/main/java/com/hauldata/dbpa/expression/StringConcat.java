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

public class StringConcat extends Expression<String> {

	public StringConcat(Expression<String> left, Expression<String> right) {
		super(VariableType.VARCHAR);
		this.left = left;
		this.right = right;
	}
	
	@Override
	public String evaluate() {
		String leftValue = left.evaluate();
		String rightValue = right.evaluate();
		
		if (leftValue == null || rightValue == null) {
			return null;
		}

		StringBuilder result = new StringBuilder();
		result.append(leftValue);
		result.append(rightValue);
		
		return result.toString();
	}

	private Expression<String> left;
	private Expression<String> right;
}
