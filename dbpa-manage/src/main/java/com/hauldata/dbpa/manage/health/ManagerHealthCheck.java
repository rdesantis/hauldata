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

package com.hauldata.dbpa.manage.health;

import com.codahale.metrics.health.HealthCheck;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.JobManagerException.NotAvailable;

public class ManagerHealthCheck extends HealthCheck {

	@Override
	protected Result check() throws Exception {

		try {
			return JobManager.getInstance().isStarted() ? Result.healthy() : Result.unhealthy("Job manager is not started");
		}
		catch (NotAvailable ex) {
			return Result.unhealthy(ex.getMessage());
		}
	}
}
