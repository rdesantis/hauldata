/*
 * Copyright (c) 2018, Ronald DeSantis
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

package com.hauldata.dbpa.task.expression.fixed;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.fixed.ValidatorFixedField;

public class ValidatorFixedFieldExpression extends ColumnFixedFieldExpression {

	Expression<String> expectedValue;

	public ValidatorFixedFieldExpression(Expression<Integer> startColumn, Expression<Integer> endColumn, Expression<String> expectedValue) {
		super(startColumn, endColumn);
		this.expectedValue = expectedValue;
	}

	@Override
	public ValidatorFixedField evaluate() {
		int[] columns = evaluateColumns();
		return new ValidatorFixedField(columns[0], columns[1], expectedValue.evaluate());
	}
}
