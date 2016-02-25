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

package com.hauldata.dbpa.control;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;

/**
 * All the information needed to launch a process and record its status
 */
public class ProcessConfiguration {

	public Integer id;
	public String processName;
	public String scriptName;
	public String propName;
	public List<SimpleEntry<String, String>> arguments;

	public ProcessConfiguration(
			Integer id,
			String name,
			String scriptName,
			String propName,
			List<SimpleEntry<String, String>> arguments) {

		this.id = id;
		this.processName = name;
		this.scriptName = scriptName;
		this.propName = propName;
		this.arguments = arguments;
	}
}
