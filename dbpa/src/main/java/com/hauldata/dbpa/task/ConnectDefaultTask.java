/*
 * Copyright (c) 2019, 2020, Ronald DeSantis
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

package com.hauldata.dbpa.task;

import java.util.Properties;

import com.hauldata.dbpa.connection.ConnectionReference;
import com.hauldata.dbpa.process.Context;

public class ConnectDefaultTask extends ConnectTask {

	public ConnectDefaultTask(
			Prologue prologue,
			ConnectionReference reference) {

		super(prologue, reference);
	}

	@Override
	protected Properties getNewProperties(Context context) {
		return getDefaultProperties(context);
	}
}
