/*
 * Copyright (c) 2018, 2019, Ronald DeSantis
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
import com.hauldata.dbpa.file.fixed.SetterFixedField;
import com.hauldata.dbpa.variable.VariableBase;

public class SetterFixedFieldExpression extends ColumnFixedFieldExpression {

	private VariableBase variable;

	public SetterFixedFieldExpression(Expression<Integer> startColumn, Expression<Integer> endColumn, VariableBase variable) {
		super(startColumn, endColumn);
		this.variable = variable;
	}

	@Override
	public SetterFixedField evaluate() {
		return new SetterFixedField(evaluateStartColumn(), evaluateEndColumn(), variable);
	}
}
