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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * File column selectors for read operations
 */
public class Columns {

	private ArrayList<int[]> sourceColumnTargetColumnIndexes;

	private static int[] noTargetColumnIndexes = new int[0];

	private static final int defaultSourceColumnCount = 255;
	private static ArrayList<int[]> defaultSourceColumnTargetColumnIndexes = getDefaultSourceColumnTargetColumnIndexes(defaultSourceColumnCount);

	private List<String> captions;
	private int size;

	/**
	 * Return the default mapping of source columns to target columns for a column list of a given size.
	 * 
	 * @param size is the number of columns in the list
	 * @return a mapping of source column positions to arrays of target column positions
	 */
	private static ArrayList<int[]> getDefaultSourceColumnTargetColumnIndexes(int size) {

		ArrayList<int[]> sourceColumnTargetColumnIndexes = new ArrayList<int[]>();
		for (int sourceColumnIndex = 1; sourceColumnIndex <= size; sourceColumnIndex++) {
			sourceColumnTargetColumnIndexes.add(new int[] {sourceColumnIndex});
		}
		return sourceColumnTargetColumnIndexes;
	}
	
	/**
	 * Constructor
	 * 
	 * @param positions is a list of Integer or String column positions.  If a position is
	 * given as a String, it must match one of the column header captions.  If positions is
	 * null or an empty list, positions are implied as the default positions of 1, 2, 3, etc.
	 * 
	 * @param headers contains the file headers as read from the file which may be empty
	 * if headers were not present.
	 */
	public Columns(List<Object> positions, Headers headers) {

		sourceColumnTargetColumnIndexes = resolvePositions(positions, headers);
		captions = determineCaptions(positions, headers);
		size = captions.size();
	}

	/**
	 * Determine numerical column indexes for column positions possibly named by header captions.
	 * 
	 * @return a mapping of source column positions to arrays of target column positions
	 */
	private ArrayList<int[]> resolvePositions(List<Object> positions, Headers headers) {

		ArrayList<int[]> sourceColumnTargetColumnIndexes = defaultSourceColumnTargetColumnIndexes;

		if (areDefault(positions)) {
			if (headers.exist()) {
				sourceColumnTargetColumnIndexes = getDefaultSourceColumnTargetColumnIndexes(headers.getCaptions().size());
			}
			return sourceColumnTargetColumnIndexes;
		}

		int[] positionColumnIndexes = new int[positions.size()];
		
		int positionIndex = 0;
		for (Object position : positions){
			if (position instanceof Integer) {
				int sourceColumnIndex = (Integer)position;
				if ((headers.exist() && (sourceColumnIndex > headers.getCaptions().size())) || (sourceColumnIndex <= 0)) {
					throw new RuntimeException("Attempting to read column beyond bounds of record at position " + String.valueOf(sourceColumnIndex));
				}
				positionColumnIndexes[positionIndex++] = sourceColumnIndex;
			}
			else if (position instanceof String) {
				if (!headers.exist()) {
					throw new RuntimeException("Attempting to read column at header \"" + (String)position + "\" but file has no headers");
				}

				int sourceColumnIndex = headers.getCaptions().indexOf((String)position) + 1;
				if (sourceColumnIndex == 0) {
					throw new RuntimeException("Attempting to read column with non-existing header \"" + (String)position + "\"");
				}
				positionColumnIndexes[positionIndex++] = sourceColumnIndex;
			}
			else {
				throw new RuntimeException("Illegal column position specification");
			}
		}

		int maxSourceColumnIndex = Arrays.stream(positionColumnIndexes).max().getAsInt();

		sourceColumnTargetColumnIndexes = new ArrayList<int[]>();
		for (int sourceColumnIndex = 1; sourceColumnIndex <= maxSourceColumnIndex; sourceColumnIndex++) {

			List<Integer> targetColumnIndexes = new LinkedList<Integer>();
			for (positionIndex = 0; positionIndex < positionColumnIndexes.length; positionIndex++) {
				if (positionColumnIndexes[positionIndex] == sourceColumnIndex) {
					targetColumnIndexes.add(positionIndex + 1);
				}
			}
			
			sourceColumnTargetColumnIndexes.add(targetColumnIndexes.stream().mapToInt(i->i).toArray());
		}

		return sourceColumnTargetColumnIndexes;
	}

	/**
	 * For a given source column position, returns an array of target column positions.
	 * 
	 * resolvePositions() must be called before the first use of getTargetColumnIndexes().
	 * 
	 * @param sourceColumnIndex is the one-based column position in the source file
	 * from which a data element is read 
	 * @return an array of one-based column positions in the target destination
	 * where the data element is to be stored, which may be an empty
	 * array if the data element is to be discarded
	 */
	public int[] getTargetColumnIndexes(int sourceColumnIndex) {

		if (sourceColumnIndex <= sourceColumnTargetColumnIndexes.size()) {
			return sourceColumnTargetColumnIndexes.get(sourceColumnIndex - 1);
		}
		else if (sourceColumnTargetColumnIndexes == defaultSourceColumnTargetColumnIndexes) {
			return new int[] {sourceColumnIndex};
		}
		else {
			return noTargetColumnIndexes;
		}
	}

	/**
	 * Determine the column captions.
	 * 
	 * @return a non-null list of column captions with possible null place holders.
	 * @see Columns#getCaptions()
	 */
	private List<String> determineCaptions(List<Object> positions, Headers headers) {

		List<String> captions = new LinkedList<String>();

		if (areDefault(positions)) {
			if (headers.exist()) {
				captions = headers.getCaptions();
			}
			return captions;
		}

		for (Object position : positions){
			if (position instanceof Integer) {
				if (headers.exist()) {
					captions.add(headers.getCaption((Integer)position - 1));
				}
				else {
					captions.add(null);
				}
			}
			else if (position instanceof String) {
				captions.add((String)position);
			}
			else {
				throw new RuntimeException("Illegal column position specification - this code should never execute");
			}
		}

		return captions;
	}

	/**
	 * @return a non-null list of column captions with possible null place holders.
	 * If the number of columns could not be deduced from the arguments, the list is empty.
	 * If the list is not empty but the actual caption at a given column position
	 * could not be determined, the corresponding list element is null.
	 */
	public List<String> getCaptions() {
		return captions;
	}

	/**
	 * @return the number of columns selected, or zero if the number of columns
	 * could not be deduced from the information passed to the constructor.
	 */
	public int size() {
		return size;
	}

	/**
	 * Set the number of columns selected, overriding the number deduced by the constructor. 
	 */
	public void setSize(int size) {
		this.size = size;
	}

	/**
	 * @return true if positions specifies the default column positions.
	 */
	private boolean areDefault(List<Object> positions) {
		return (positions == null) || positions.isEmpty();
	}
}
