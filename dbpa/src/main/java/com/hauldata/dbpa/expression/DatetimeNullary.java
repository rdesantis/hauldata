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

import java.time.LocalDateTime;

import com.hauldata.dbpa.variable.VariableType;

public class DatetimeNullary extends Expression<LocalDateTime> {

	/**
	 * Instantiate a function call that takes no arguments and returns Datetime.
	 * Currently only GETDATE() is supported, so the constructor does not need
	 * a parameter to indicate which function to invoke.
	 */
	public DatetimeNullary() {
		super(VariableType.DATETIME);
	}
	
	@Override
	public LocalDateTime evaluate() {
		return LocalDateTime.now();
	}

}
