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

public class BooleanFromStrings extends Expression<Boolean> {

	public BooleanFromStrings(Expression<String> left, Expression<String> right, Comparison comparison) {
		super(VariableType.BOOLEAN);
		this.left = left;
		this.right = right;
		this.comparison = comparison;
	}
	
	@Override
	public Boolean evaluate() {
		String leftValue = left.evaluate();
		String rightValue = right.evaluate();

		if (leftValue == null || rightValue == null) {
			return false;
		}

		int comparisonResult = leftValue.compareToIgnoreCase(rightValue);

		switch (comparison) {
		default:
		case lt: return comparisonResult < 0;
		case le: return comparisonResult <= 0;
		case eq: return comparisonResult == 0;
		case ne: return comparisonResult != 0;
		case ge: return comparisonResult >= 0;
		case gt: return comparisonResult > 0;
		}
	}

	private Expression<String> left;
	private Expression<String> right;
	private Comparison comparison;
}
