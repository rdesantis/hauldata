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

package com.hauldata.dbpa.task;

import java.util.Map;

import com.hauldata.dbpa.connection.Connection;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;
import com.hauldata.util.schedule.ScheduleSet;

public class OnTask extends ScheduleTask {

	private ScheduleSet schedules;

	public OnTask(
			Prologue prologue,
			ScheduleSet schedules,
			NestedTaskSet taskSet,
			Map<String, Connection> connections) {

		super(prologue, taskSet, connections);
		this.schedules = schedules;
	}

	@Override
	protected void execute(Context context) {
		execute(context, schedules);
	}
}
