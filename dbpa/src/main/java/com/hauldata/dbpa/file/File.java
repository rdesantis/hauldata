/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

package com.hauldata.dbpa.file;

import java.nio.file.Path;

public abstract class File extends Node {

	private FileOptions options;

	public static abstract class Owner implements Node.Owner {

		public abstract File put(Path path, File file);
		public abstract File get(Path path);
		public abstract File remove(Path path);

		@Override
		public Node put(Object key, Node node) {
			return put((Path)key, (File)node);
		}
		@Override
		public Node get(Object key) {
			return get((Path)key);
		}
		@Override
		public Node remove(Object key) {
			return remove((Path)key);
		}
	}

	protected File(Owner owner, Path path, FileOptions options) {
		super(owner, path);
		this.options = options;
	}

	protected File(Owner owner, Path path) {
		this(owner, path, null);
	}

	@Override
	public String getName() {
		return ((Path)key).toString();
	}

	public FileOptions getOptions() {
		return options;
	}
}
