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

package com.hauldata.dbpa.variable;

public class Variable<Type> extends VariableBase {

	/**
	 * Create a variable
	 * 
	 * @param name is the variable name
	 * @param example is a non-null example of the type; any arbitrary value can be passed
	 */
	public Variable(String name, VariableType type) {
		super(name, type);
	}

	public void setValue(Type value) {
		setValueObject(value);
	}

	@SuppressWarnings("unchecked")
	public Type getValue() {
		return (Type)getValueObject();
	}
}
