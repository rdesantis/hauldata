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

import com.hauldata.dbpa.control.Controller;
import com.hauldata.dbpa.control.resources.ServiceResource;

public class ControlDbp extends Application<Configuration>{

	public static void main(String[] args) throws Exception {

		new ControlDbp().run(args);

		Controller.getInstance().close();
	}

    @Override
    public String getName() {
        return "dbpa-control";
    }

    @Override
    public void initialize(Bootstrap<Configuration> bootstrap) {
        // Nothing to do yet.
    	// Eventually may subclass Configuration as ServiceConfiguration and receive that here.
    }

	@Override
	public void run(Configuration configuration, Environment environment) {

		// Eventually may subclass Configuration as ServiceConfiguration and use members
		// as parameters when instantiating resources.

		final ServiceResource resource = new ServiceResource();

		// TODO: Register health check(s) here.

		environment.jersey().register(resource);
	}
}
