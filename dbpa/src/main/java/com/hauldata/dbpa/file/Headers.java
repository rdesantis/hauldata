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

import java.util.ArrayList;

public abstract class Headers {

	protected boolean exist;
	protected ArrayList<String> captions;
	protected int columnCount;

	public Headers() {

		this.exist = false;
		this.captions = new ArrayList<String>();
		columnCount = 0;
	}

	public Headers(
			boolean exist,
			ArrayList<String> captions) {

		this.exist = exist;
		this.captions = captions;
		columnCount = captions.size();
	}

	public boolean exist() {
		return exist;
	}

	public ArrayList<String> getCaptions() {
		return captions;
	}

	public String getCaption(int i) {
		return captions.get(i);
	}

	public void setCaptions(ArrayList<String> captions) {
		this.captions = captions;
		columnCount = captions.size();
	}

	public int getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(int columnCount) {
		if (this.columnCount != 0) {
			throw new RuntimeException("Internal error - attempt to change number of columns");
		}

		this.columnCount = columnCount;
	}

}
