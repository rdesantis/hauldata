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
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.exceptions.DefaultExceptionMapper;
import com.hauldata.dbpa.manage.exceptions.FileNotFoundExceptionMapper;
import com.hauldata.dbpa.manage.exceptions.JobManagerExceptionMapper;
import com.hauldata.dbpa.manage.exceptions.NameNotFoundExceptionMapper;
import com.hauldata.dbpa.manage.exceptions.NoSuchElementExceptionMapper;
import com.hauldata.dbpa.manage.exceptions.NoSuchFileExceptionMapper;
import com.hauldata.dbpa.manage.exceptions.RuntimeExceptionMapper;
import com.hauldata.dbpa.manage.health.ManagerHealthCheck;
import com.hauldata.dbpa.manage.health.SchemaHealthCheck;
import com.hauldata.dbpa.manage.resources.JobsResource;
import com.hauldata.dbpa.manage.resources.ManagerResource;
import com.hauldata.dbpa.manage.resources.PropertiesFilesResource;
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

		JobManager.instantiate(false);

		// Eventually may subclass Configuration as ServiceConfiguration and receive that here
		// to control startup behavior.
    }

	@Override
	public void run(Configuration configuration, Environment environment) {

		// Eventually may subclass Configuration as ServiceConfiguration and use members
		// as parameters when instantiating resources.

		final ServiceResource service = new ServiceResource();
		final ManagerResource manager = new ManagerResource();
		final SchemaResource schema = new SchemaResource();
		final ScriptsResource scripts = new ScriptsResource();
		final PropertiesFilesResource propFiles = new PropertiesFilesResource();
		final SchedulesResource schedules = new SchedulesResource();
		final JobsResource jobs = new JobsResource();

		// Health checks

		environment.healthChecks().register("schema", new SchemaHealthCheck());
		environment.healthChecks().register("manager", new ManagerHealthCheck());

		// Exception mappers

		environment.jersey().register(new DefaultExceptionMapper());
		environment.jersey().register(new FileNotFoundExceptionMapper());
		environment.jersey().register(new JobManagerExceptionMapper());
		environment.jersey().register(new NameNotFoundExceptionMapper());
		environment.jersey().register(new NoSuchElementExceptionMapper());
		environment.jersey().register(new NoSuchFileExceptionMapper());
		environment.jersey().register(new RuntimeExceptionMapper());

		// Resources

		environment.jersey().register(service);
		environment.jersey().register(manager);
		environment.jersey().register(schema);
		environment.jersey().register(scripts);
		environment.jersey().register(propFiles);
		environment.jersey().register(schedules);
		environment.jersey().register(jobs);

		// Managed objects

		environment.lifecycle().manage(new ManagedJobManager());
	}

	static {QuietLog4j.please();}
}

class ManagedJobManager implements Managed {

	@Override
	public void start() throws Exception {
		// According to http://www.dropwizard.io/0.6.2/maven/apidocs/com/yammer/dropwizard/lifecycle/Managed.html,
		// throwing an exception here should "halt the service startup".  However, it is observed in practice that the
		// server continues to run.  It does not terminate.  So force termination.

		try { actualStart(); }
		catch (Exception ex) {
			ex.printStackTrace();
			System.exit(1);
		}
	}

	private void actualStart() throws Exception {
		JobManager manager = JobManager.getInstance();
		if (manager.canStartup()) {
			manager.startup();
		}
	}

	@Override
	public void stop() throws Exception {
		JobManager manager = JobManager.getInstance();
		if (manager.isStarted()) {
			manager.shutdown();
		}
	}
}
