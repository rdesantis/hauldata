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

package com.hauldata.dbpa.file;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Book extends File implements Node.Owner {

	protected Map<String, Sheet> sheets;

	public Book(Owner owner, Path path) {
		super(owner, path);
		sheets = new ConcurrentHashMap<String, Sheet>();
	}

	@Override
	public Node put(Object key, Node node) {
		return sheets.put((String)key, (Sheet)node);
	}

	@Override
	public Node get(Object key) {
		return sheets.get((String)key);
	}

	@Override
	public Node remove(Object key) {
		return sheets.remove((String)key);
	}
}
