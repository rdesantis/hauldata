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

import java.util.List;
import java.util.Map;

public class SimpleCase<Type> extends Expression<Type> {

	private ExpressionBase input;
	private List<Map.Entry<ExpressionBase, Expression<Type>>> whenClauses;
	private Expression<Type> elseResult;

	public SimpleCase(
			ExpressionBase input,
			List<Map.Entry<ExpressionBase, Expression<Type>>> whenClauses,
			Expression<Type> elseResult) {
		super(whenClauses.get(0).getValue().getType());
		this.input = input;
		this.whenClauses = whenClauses;
		this.elseResult = elseResult;
	}

	@Override
	public Type evaluate() {

		Object inputValue = input.getEvaluationObject();

		if (inputValue != null) {
			for (Map.Entry<ExpressionBase, Expression<Type>> whenClause : whenClauses) {
				if (inputValue.equals(whenClause.getKey().getEvaluationObject())) {
					return whenClause.getValue().evaluate();
				}
			}
		}

		return (elseResult != null) ? elseResult.evaluate() : null;
	}
}
