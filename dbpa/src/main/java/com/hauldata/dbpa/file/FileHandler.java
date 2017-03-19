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

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class FileHandler {

	private String name;
	private boolean hasSheets;
	private TargetPage.Factory targetFactory;
	private SourcePage.Factory sourceFactory;

	private FileHandler(
			String name,
			boolean hasSheets,
			TargetPage.Factory targetFactory,
			SourcePage.Factory sourceFactory) {
		this.name = name;
		this.hasSheets = hasSheets;
		this.targetFactory = targetFactory;
		this.sourceFactory = sourceFactory;
	}

	// Static members

	static private Map<String, FileHandler> handlers = new HashMap<String, FileHandler>();

	/**
	 * Register a file handler for a specific file type.
	 * @param name is the type name, e.g., 'CSV', 'XLSX'
	 * @param hasSheets is true if the file type supports multiple named sheets in a file
	 * @param targetFactory is a factory that instantiates WritePage objects for the type
	 * @param sourceFactory is a factory that instantiates ReadPage objects for the type
	 */
	static public void register(
			String name,
			boolean hasSheets,
			TargetPage.Factory targetFactory,
			SourcePage.Factory sourceFactory) {

		handlers.put(name, new FileHandler(name, hasSheets, targetFactory, sourceFactory));
	}

	/**
	 * Get the file handler for a type name.
	 * 
	 * @param name is a file type name
	 * @param writeNotRead is true if the file will be written, false if it will be read
	 * @return the file handler for the type
	 * @throws NoSuchElementException if a handler for this type was not registered
	 * @throws UnsupportedOperationException if the handler does not support the specified write or read operation.
	 */
	static public FileHandler get(String name, boolean writeNotRead) {
		FileHandler handler = handlers.get(name);
		if (handler == null) {
			throw new NoSuchElementException("Unrecognized file type \"" + name + "\"");
		}
		else if (writeNotRead && handler.targetFactory == null) {
			throw new UnsupportedOperationException("Write to \"" + name + "\" is not supported");
		}
		else if (!writeNotRead && handler.sourceFactory == null) {
			throw new UnsupportedOperationException("Read from \"" + name + "\" is not supported");
		}
		return handler;
	}

	// Non-static members

	public String getName() {
		return name;
	}

	public boolean getHasSheets() {
		return hasSheets;
	}

	public TargetPage.Factory getTargetFactory() {
		return targetFactory;
	}

	public SourcePage.Factory getSourceFactory() {
		return sourceFactory;
	}
}
