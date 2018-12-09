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

package com.hauldata.dbpa.file.fixed;

public class KeeperFixedField extends FixedField {

	public static class Keeper implements Actor {

		private String value;

		public Keeper() {}

		@Override
		public void invokeWith(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}
	}

	private boolean joined;

	public KeeperFixedField(int startColumn, int endColumn, boolean joined) {
		super(startColumn, endColumn, new Keeper());
		this.joined = joined;
	}

	public boolean isJoined() {
		return joined;
	}

	public String getValue() {
		return ((Keeper)actor).getValue();
	}
}
