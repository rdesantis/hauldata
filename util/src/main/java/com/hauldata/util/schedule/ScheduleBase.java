/*
 * Copyright (c) 2016-2017, 2019, Ronald DeSantis
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

package com.hauldata.util.schedule;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;

public abstract class ScheduleBase {

	private boolean immediate;

	protected ScheduleBase(boolean immediate) {
		this.immediate = immediate;
	}

	/**
	 * @return true if the next scheduled event is immediate.
	 */
	public boolean isImmediate() {
		return immediate;
	}

	public void setImmediate(boolean immediate) {
		this.immediate = immediate;
	}

	/**
	 * Return the datetime of the next scheduled event on or after
	 * the indicated datetime.
	 * <p>
	 * Because the mapping from a LocalTime value to an Instant may be
	 * ambiguous on a day when Daylight Saving Time starts or ends,
	 * this method cannot be used to reliably determine the elapsed time
	 * until the next scheduled event will occur.  Use the method
	 * nextFrom(ZonedDateTime) if the method result is to be used for
	 * that purpose.
	 *
	 * @param earliest is the earliest datetime allowed for the next event
	 * @return the datetime of the next event or null if no more events are
	 * scheduled on or after the datetime
	 */
	public abstract LocalDateTime nextFrom(LocalDateTime earliest);

	/**
	 * Return the datetime of the next scheduled event on or after
	 * the indicated datetime.
	 *
	 * @param earliest is the earliest datetime allowed for the next event
	 * @return the datetime of the next event or null if no more events are
	 * scheduled on or after the datetime
	 */
	public abstract ZonedDateTime nextFrom(ZonedDateTime earliest);
}
