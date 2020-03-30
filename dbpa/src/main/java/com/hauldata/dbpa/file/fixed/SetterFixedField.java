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

package com.hauldata.dbpa.file.fixed;

import com.hauldata.dbpa.variable.VariableBase;
import com.hauldata.dbpa.variable.VariablesFromArguments;

public class SetterFixedField extends ColumnFixedField {

	public static class VariableSetter implements Actor {

		private VariableBase variable;

		public VariableSetter(VariableBase variable) {
			this.variable = variable;
		}

		@Override
		public void invokeWith(int lineNumber, String value) {
			VariablesFromArguments.set(variable, value);
		}
	}

	public SetterFixedField(int startColumn, int endColumn, VariableBase variable) {
		super(startColumn, endColumn, new VariableSetter(variable));
	}
}
