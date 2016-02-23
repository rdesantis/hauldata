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

public class IfExpression<Type> extends Expression<Type> {

	public IfExpression(Expression<Boolean> condition, Expression<Type> left, Expression<Type> right) {
		super(left.getType());
		this.condition = condition;
		this.left = left;
		this.right = right;
	}
	
	@Override
	public Type evaluate() {
		return condition.evaluate() ? left.evaluate() : right.evaluate();
	}

	private Expression<Boolean> condition;
	private Expression<Type> left;
	private Expression<Type> right;
}
