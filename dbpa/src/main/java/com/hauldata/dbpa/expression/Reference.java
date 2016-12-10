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

import com.hauldata.dbpa.variable.Variable;
import com.hauldata.dbpa.variable.VariableBase;

public class Reference<Type> extends Expression<Type> {

	private Variable<Type> variable;

	public Reference(Variable<Type> variable) {
		super(variable.getType());
		this.variable = variable;
	}

	@Override
	public Type evaluate() {
		return variable.getValue();
	}

	public VariableBase getVariable() {
		return variable;
	}
}
