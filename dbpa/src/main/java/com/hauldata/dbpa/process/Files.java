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

package com.hauldata.dbpa.process;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.hauldata.dbpa.file.File;

public class Files extends File.Owner {

	private Map<Path, File> files;

	private static String separator = FileSystems.getDefault().getSeparator();

	public Files() {
		files = new ConcurrentHashMap<Path, File>();
	}

	// File.Owner overrides

	@Override
	public File put(Path path, File file) {
		return files.put(path, file);
	}

	@Override
	public File get(Path path) {
		return files.get(path);
	}

	@Override
	public File remove(Path path) {
		return files.remove(path);
	}

	// Static members
	
	/**
	 * Get the fully-resolved normalized path of a file name that may include a relative or absolute path.
	 * Uses the provided parentName to resolve relative paths.
	 * @param parentName is the parent path for resolving relative file names.
	 * @param fileName is the file name to resolve.
	 * @return the fully-resolved path
	 */
	public static Path getPath(String parentName, String fileName) {
		return Paths.get(parentName).resolve(fileName).toAbsolutePath().normalize();
	}

	/**
	 * Get just the trailing file name portion of a path.
	 * @param fileName is the potentially qualified file name from which the trailing portion is extracted
	 * @return the trailing file name portion
	 */
	public static String getFileName(String fileName) {
		return getParentAndFileName(fileName)[1];
	}

	/**
	 * Split a file name into leading and trailing portions, the trailing portion of which follows the last
	 * file separator character in the name.  No test is done on whether any component is a valid name in the file system.
	 * If there is no file separator in the name, the leading portion is returned as '.'.  
	 * @param fileName is the potentially qualified file name from which leading and trailing portions are extracted
	 * @return an array of 2 strings, the element 0 containing the leading portion and element 1 containing the trailing portion.
	 */
	public static String[] getParentAndFileName(String fileName) {

		// Paths#get(String) does not accept the wildcard character so it can't be used here.
		// On MS Windows, the file separator is backslash but forward slash is also accepted.
		// Allow the user to specify either. 

		int lastSeparatorIndex = Math.max(
				fileName.lastIndexOf(separator),
				fileName.lastIndexOf("/"));
		if (lastSeparatorIndex == -1) {
			return new String[] { "." , fileName };
		}
		else {
			return new String[] {
					fileName.substring(0, lastSeparatorIndex),
					fileName.substring(lastSeparatorIndex + 1) };
		}
	}

	// Class members

	public boolean contains(Path path) {
		return files.containsKey(path);
	}

	/**
	 * Check if the indicated file path is currently open in the context, and if so, close it.
	 * @param path is the path name of the file to assure is closed.
	 */
	public void assureNotOpen(Path path) {

		File file = files.get(path);
		if (file != null) {
			if (file.isOpen()) {
				try {
					file.close();
				} catch (Exception e) {}
			}
			files.remove(path);
		}
	}

	/**
	 * Close all files that are currently open in the context.
	 */
	public void assureAllClosed() {

		for (File file : files.values()) {
			if (file.isOpen()) {
				try {
					file.close();
				} catch (Exception e) {}
			}
		}
	}
}
