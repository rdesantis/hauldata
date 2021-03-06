/*
 * Copyright (c) 2016, 2018, Ronald DeSantis
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
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.cli.Command;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.eclipse.jetty.servlet.ErrorPageErrorHandler;

import com.hauldata.dbpa.dropwizard.CorsConfigurator;
import com.hauldata.dbpa.dropwizard.ManageDbpConfiguration;
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
import com.hauldata.dbpa.manage.resources.SourceFilesResource;
import com.hauldata.dbpa.manage.resources.TargetFilesResource;

public class ManageDbp extends Application<ManageDbpConfiguration> {

	public static void main(String[] args) throws Exception {
		new ManageDbp().run(args);
	}

    @Override
    public String getName() {
        return "dbpa-manage";
    }

    @Override
    public void initialize(Bootstrap<ManageDbpConfiguration> bootstrap) {

		JobManager.instantiate(false);

		bootstrap.addCommand(new ResetCommand());

		// Serve static assets

		bootstrap.addBundle(new AssetsBundle("/assets/", "/", "index.html"));
    }

	@Override
	public void run(ManageDbpConfiguration configuration, Environment environment) {

		// Cross-origin resource sharing (CORS)

		CorsConfigurator corsConfigurator = configuration.getCorsConfigurator();
		if (corsConfigurator.getEnabled()) {
			corsConfigurator.enableCors(environment);
		}

		// Redirect 404 errors to index.html

		ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
		errorHandler.addErrorPage(404, "/index.html");
		environment.getApplicationContext().setErrorHandler(errorHandler);

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

		final ServiceResource service = new ServiceResource();
		final ManagerResource manager = new ManagerResource();
		final SchemaResource schema = new SchemaResource();
		final ScriptsResource scripts = new ScriptsResource();
		final PropertiesFilesResource propFiles = new PropertiesFilesResource();
		final SourceFilesResource sourceFiles = new SourceFilesResource();
		final TargetFilesResource targetFiles = new TargetFilesResource();
		final SchedulesResource schedules = new SchedulesResource();
		final JobsResource jobs = new JobsResource();

		environment.jersey().register(service);
		environment.jersey().register(manager);
		environment.jersey().register(schema);
		environment.jersey().register(scripts);
		environment.jersey().register(propFiles);
		environment.jersey().register(sourceFiles);
		environment.jersey().register(targetFiles);
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

class ResetCommand extends Command {

	protected ResetCommand() {
		super("reset", "Resets Job Manager database schema from 'in use' to 'available'");
	}

	@Override
	public void configure(Subparser arg0) {
		// This command has no options to be parsed.
	}

	@Override
	public void run(Bootstrap<?> arg0, Namespace arg1) {

		try {
			JobManager manager = JobManager.getInstance();
			manager.reset();
		}
		catch (Exception ex) {
			System.out.println(ex.toString());
		}

		// Dropwizard does not terminate the application when the commands have finished executing.
		// Kill it.

		System.exit(0);
	}
}
