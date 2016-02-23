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

public class BooleanBinary extends Expression<Boolean> {

	public BooleanBinary(Expression<Boolean> left, Expression<Boolean> right, Combination operator) {
		super(VariableType.BOOLEAN);
		this.left = left;
		this.right = right;
		this.operator = operator;
	}
	
	@Override
	public Boolean evaluate() {
		boolean leftValue = left.evaluate();
		boolean rightValue = right.evaluate();
		
		switch (operator) {
		default:
		case and: return leftValue && rightValue;
		case or: return leftValue || rightValue;
		}
	}

	private Expression<Boolean> left;
	private Expression<Boolean> right;
	private Combination operator;
}
