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

public class IntegerBinary extends Expression<Integer> {

	public enum Operator { add, subtract, multiply, divide, modulo };
	
	public IntegerBinary(Expression<Integer> left, Expression<Integer> right, Operator operator) {
		super(VariableType.INTEGER);
		this.left = left;
		this.right = right;
		this.operator = operator;
	}
	
	@Override
	public Integer evaluate() {
		Integer leftValue = left.evaluate();
		Integer rightValue = right.evaluate();

		if (leftValue == null || rightValue == null) {
			return null;
		}

		switch (operator) {
		default:
		case add: return leftValue + rightValue;
		case subtract: return leftValue - rightValue;
		case multiply: return leftValue * rightValue;
		case divide: return leftValue / rightValue;
		case modulo: return leftValue % rightValue;
		}
	}

	private Expression<Integer> left;
	private Expression<Integer> right;
	private Operator operator;
}
