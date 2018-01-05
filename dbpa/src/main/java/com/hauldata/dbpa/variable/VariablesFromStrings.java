/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.dbpa.variable;

import java.util.Iterator;
import java.util.List;

import com.hauldata.dbpa.expression.DatetimeFromString;
import com.hauldata.dbpa.expression.StringConstant;

public class VariablesFromStrings {

	public static void set(List<VariableBase> variables, String[] args) {

		int n = Math.min(variables.size(), args.length);
		Iterator<VariableBase> parameterIterator = variables.iterator();
		for (int i = 0; i < n; ++i) {
			set(parameterIterator.next(), args[i]);
		}
	}

	public static void set(VariableBase variable, String arg) {

		if (arg == null) {
			variable.setValueObject(null);
		}
		else if (variable.getType() == VariableType.INTEGER || variable.getType() == VariableType.BIT) {
			try {
				variable.setValueChecked(Integer.valueOf(arg));
			}
			catch (NumberFormatException ex) {
				throw new RuntimeException("Invalid integer format: " + arg);
			}
		}
		else if (variable.getType() == VariableType.VARCHAR) {
			variable.setValueObject(arg);
		}
		else if (variable.getType() == VariableType.DATETIME) {
			DatetimeFromString converter = new DatetimeFromString(new StringConstant(arg));
			variable.setValueObject(converter.evaluate());
		}
	}
}
