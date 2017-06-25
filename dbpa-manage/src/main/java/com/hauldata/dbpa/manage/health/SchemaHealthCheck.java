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
import com.hauldata.dbpa.manage.resources.SchemaResource;

public class SchemaHealthCheck extends HealthCheck {

	@Override
	protected Result check() throws Exception {

		SchemaResource schemaResource = new SchemaResource();

		try {
			return schemaResource.confirm() ? Result.healthy(): Result.unhealthy("Full DBPA job management Schema does not exist");
		}
		catch (RuntimeException ex) {
			return Result.unhealthy(ex.getMessage());
		}
	}
}
