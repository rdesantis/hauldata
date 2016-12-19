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

package com.hauldata.dbpa;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.sql.SQLException;

import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.resources.JobsResource;
import com.hauldata.dbpa.manage.resources.ManagerResource;
import com.hauldata.dbpa.manage.resources.PropertiesGroupsResource;
import com.hauldata.dbpa.manage.resources.SchedulesResource;
import com.hauldata.dbpa.manage.resources.SchemaResource;
import com.hauldata.dbpa.manage.resources.ScriptsResource;
import com.hauldata.dbpa.manage.resources.ServiceResource;

public class ManageDbp extends Application<Configuration> {

	public static void main(String[] args) throws Exception {
		new ManageDbp().run(args);
	}

    @Override
    public String getName() {
        return "dbpa-manage";
    }

    @Override
    public void initialize(Bootstrap<Configuration> bootstrap) {

		JobManager manager = JobManager.instantiate(false);

		try {
			manager.startup();
		}
		catch (SQLException e) {
			// Fatal error: can't do database I/O.

			System.exit(e.getErrorCode());
		}

		// Eventually may subclass Configuration as ServiceConfiguration and receive that here
		// to control startup behavior.
    }

	@Override
	public void run(Configuration configuration, Environment environment) {

		// Eventually may subclass Configuration as ServiceConfiguration and use members
		// as parameters when instantiating resources.

		final ServiceResource service = new ServiceResource(environment);
		final ManagerResource manager = new ManagerResource();
		final SchemaResource schema = new SchemaResource();
		final ScriptsResource scripts = new ScriptsResource();
		final PropertiesGroupsResource propFiles = new PropertiesGroupsResource();
		final SchedulesResource schedules = new SchedulesResource();
		final JobsResource jobs = new JobsResource();

		// TODO: Register health check(s) here.

		environment.jersey().register(service);
		environment.jersey().register(manager);
		environment.jersey().register(schema);
		environment.jersey().register(scripts);
		environment.jersey().register(propFiles);
		environment.jersey().register(schedules);
		environment.jersey().register(jobs);
	}
}
