/*
 * Copyright (c) 2018, Ronald DeSantis
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

package com.hauldata.dbpa.manage;

import java.util.Properties;

import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public abstract class JobStatisticsCollector {

	public static class WithWarning {
		StatsDClient statsd;
		String warning;

		WithWarning(StatsDClient statsd, String warning) {
			this.statsd = statsd;
			this.warning = warning;
		}
	}

	/**
	 * @param properties contains properties for initializing a statsd client
	 *
	 * @return a JobStatisticsCollector.WithWarning with fields set as follows:
	 * <br>
	 * <code>statsd</code> returns a NonBlockingStatsDClient if all required properties are found;
	 * otherwise returns a NoOpStatsDClient.
	 * <br>
	 * <code>warning</code> returns null if statsd returned a NonBlockingStatsDClient;
	 * otherwise returns a message explaining why a NoOpStatsDClient was returned.
	 */
	public static JobStatisticsCollector.WithWarning create(Properties properties) {

		if (properties == null) {
			return new WithWarning(new NoOpStatsDClient(), "no properties file");
		}

		String prefix = properties.getProperty("statsd.prefix");
		String hostname = properties.getProperty("statsd.hostname");
		String port = properties.getProperty("statsd.port");

		if ((prefix == null) || (hostname == null) || (port == null)) {
			return new WithWarning(new NoOpStatsDClient(), "statsd.prefix, statsd.hostname, or statsd.port not found in properties file");
		}

		try {
			int portNumber = Integer.parseInt(port);

			return new WithWarning(new NonBlockingStatsDClient(prefix, hostname, portNumber), null);
		}
		catch (Exception ex) {
			return new WithWarning(new NoOpStatsDClient(), ex.toString());
		}
	}
}
