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

package com.hauldata.dbpa.task;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

import com.hauldata.dbpa.process.Context;

public abstract class RenameOrCopyTask extends Task {

	public RenameOrCopyTask(Prologue prologue) {
		super(prologue);
	}

	abstract protected String verb();
	
	abstract protected void action(Path from, Path to) throws IOException;
	
	protected void doAction(Context context, Path from, Path to) {
		
		try {
			context.files.assureNotOpen(from);
			context.files.assureNotOpen(to);

			action(from, to);
		}
		catch (FileAlreadyExistsException | DirectoryNotEmptyException ex) {
			throw new RuntimeException("Cannot " + verb() + " " + from.toString() + " because " + to.toString() + " already exists");
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to " + verb() + " " + from.toString() + " to " + to.toString() + " - " + message, ex);
		}
	}
}
