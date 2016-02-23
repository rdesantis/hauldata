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

public class VariableBase {

	protected VariableBase(String name, VariableType type) {
		this.name = name;
		this.type = type;
		this.value = null;
	}

	public String getName() {
		return name;
	}

	public VariableType getType() {
		return type;
	}

	public Object getValueObject() {
		return value;
	}

	public void setValueObject(Object valueObject) {
		this.value = valueObject;
	}

	/**
	 * Set variable value, supporting any valid automatic type conversions,
	 * but throwing an exception if attempting to set from an unsupportable type.
	 * 
	 * @param valueObject is the new value to set
	 */
	public void setValueChecked(Object valueObject) {
		setValueObject(type.getValueChecked(valueObject));
	}
	
	private String name;
	private VariableType type;
	private Object value;
}

