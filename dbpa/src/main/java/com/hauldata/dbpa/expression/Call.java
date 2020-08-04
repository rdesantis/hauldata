/*
 * Copyright (c) 2020, Ronald DeSantis
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import com.hauldata.dbpa.variable.Table;
import com.hauldata.dbpa.variable.VariableType;

public class Call<Type> extends Expression<Type> {

	private Expression<String> className;
	private Expression<String> methodName;
	private List<ExpressionBase> expressions;

	public Call(VariableType type, Expression<String> className, Expression<String> methodName, List<ExpressionBase> expressions) {
		super(type);
		this.className = className;
		this.methodName = methodName;
		this.expressions = expressions;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Type evaluate() {

		String className = this.className.evaluate();
		String methodName = this.methodName.evaluate();
		Object[] arguments = new Object[expressions.size()];

		int i = 0;
		for (ExpressionBase expression : expressions) {
			Object argument = expression.getEvaluationObject();
			if (argument instanceof Table) {
				argument = ((Table) argument).getValuesLists();
			}
			arguments[i++] = argument;
		}

		try {
			Class<?>[] parameterTypes = new Class[arguments.length];
			for (int j = 0; j < arguments.length; ++j) {
				if (arguments[j] instanceof List) {
					parameterTypes[j] = List.class;
				}
				else {
					parameterTypes[j] = arguments[j].getClass();
				}
			}

			Class<?> callClass = Class.forName(className);
			Method callMethod = callClass.getMethod(methodName, parameterTypes);

			Object result = callMethod.invoke(null, arguments);

			if (getType() == VariableType.TABLE) {
				result = new Table((List<List<Object>>)result);
			}

			return (Type)result;
		}
		catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InvocationTargetException | IllegalAccessException | IllegalArgumentException ex) {
			throw new RuntimeException(
					"Error loading or invoking class " + String.valueOf(className) + " method " + String.valueOf(methodName) + " - " + ex.toString());
		}
	}
}
