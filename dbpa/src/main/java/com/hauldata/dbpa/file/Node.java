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

import java.io.IOException;

public abstract class Node {

	public static interface Owner {
		Node put(Object key, Node node);
		Node get(Object key);
		Node remove(Object key);
	}

	public static interface Factory {
		Node instantiate(Owner owner, Object key, FileOptions options);
		String getTypeName();
	}

	protected Owner owner;
	protected Object key;

	private boolean isOpen;
	private boolean isWritable;
	private boolean isReadable;

	/**
	 * Constructor
	 *  
	 * @param parent is the collection that owns this node
	 * @param key is the key that uniquely identifies this node to its parent 
	 */
	protected Node(Owner owner, Object key) {
		this.owner = owner;
		this.key = key;

		this.isOpen = false;
		this.isWritable = false;
		this.isReadable = false;
	}

	protected void setState(boolean isOpen, boolean isWritable, boolean isReadable) {
		this.isOpen = isOpen;
		this.isWritable = isWritable;
		this.isReadable = isReadable;
	}

	public boolean isOpen() {
		return isOpen;
	}

	public boolean isWritable() {
		return isWritable;
	}

	public boolean isReadable() {
		return isReadable;
	}

	public void setOpen(boolean isOpen) {
		this.isOpen = isOpen;
	}

	/**
	 * @return the node type name, e.g., File, Sheet
	 */
	public abstract String getTypeName();

	/**
	 * @return the human-readable rendering of the unique key
	 */
	public abstract String getName();

	// Physical operations

	public abstract void create() throws IOException;
	public abstract void append() throws IOException;
	public abstract void open() throws IOException;
	public abstract void load() throws IOException;
	public abstract void close() throws IOException;

	/**
	 * Return the node with status as though the node had been created for writing.
	 * Throw an exception if it is not logically acceptable for the node to be created.
	 * 
	 * isOpen() on the node returns false.  The caller must physically create the entity
	 * represented by the node and upon success call setOpen(true) on the node.
	 */
	public static Node getForCreate(Owner owner, Object key, Factory factory, FileOptions options) {
		return get(owner, key, false, true, factory, options, "create", true);
	}

	/**
	 * Return the node with status as though the node had been positioned for appending.
	 * Throw an exception if it is not logically acceptable for the node to be appended.
	 * 
	 * If isOpen() on the node returns false, the caller must physically create the entity
	 * represented by the node and upon success call setOpen(true) on the node.
	 */
	public static Node getForAppend(Owner owner, Object key, Factory factory) {
		return get(owner, key, false, false, factory, null, "append", true);
	}

	/**
	 * Return the node with status as though the node had been created for writing
	 * or positioned for appending, depending on whether the node has already been created.
	 * Throw an exception if it is not logically acceptable for the node to be appended.
	 * 
	 * If isOpen() on the node returns false, the caller must physically create the entity
	 * represented by the node and upon success call setOpen(true) on the node.
	 */
	public static Node getForWrite(Owner owner, Object key, Factory factory, FileOptions options) {
		return get(owner, key, false, false, factory, options, "write", true);
	}

	/**
	 * Return the node with status as though the node had been opened for reading.
	 * Throw an exception if it is not logically acceptable for the node to be opened.
	 * 
	 * isOpen() on the node returns false.  The caller must physically open the entity
	 * represented by the node and upon success call setOpen(true) on the node.
	 */
	public static Node getForOpen(Owner owner, Object key, Factory factory) {
		return get(owner, key, false, true, factory, null, "re-open", false);
	}

	/**
	 * Return the node with status as though the node had been positioned for loading.
	 * Throw an exception if it is not logically acceptable for the node to be loaded.
	 * 
	 * isOpen() on the node returns true.
	 */
	public static Node getForLoad(Owner owner, Object key, Factory factory) {
		return get(owner, key, true, false, factory, null, "load", false);
	}

	/**
	 * Return the node with status as though the node had been opened for reading
	 * or positioned for loading, depending on whether the node has already been opened.
	 * Throw an exception if it is not logically acceptable for the node to be read.
	 * 
	 * If isOpen() on the node returns false, the caller must physically open the entity
	 * represented by the node and upon success call setOpen(true) on the node.
	 */
	public static Node getForRead(Owner owner, Object key, Factory factory) {
		return get(owner, key, false, false, factory, null, "read", false);
	}

	/**
	 * Factor out the common code from the three functions above.
	 */
	private static Node get(
			Owner owner,
			Object key,
			boolean mustBeOpen,
			boolean cantBeOpen,
			Factory factory,
			FileOptions options,
			String verb,
			boolean writableNotReadable) {
		
		Node node = (Node)owner.get(key);
		boolean nodeIsOpen = (node != null) && node.isOpen();

		if ((nodeIsOpen && cantBeOpen) || (!nodeIsOpen && mustBeOpen)) {
			throwRuntimeException(factory.getTypeName(), (cantBeOpen ? "already" : "not") + " open", verb, key);
		}
		if (nodeIsOpen && (writableNotReadable != node.isWritable())) {
			throwRuntimeException(factory.getTypeName(), "open for " + (writableNotReadable ? "reading" : "writing"), verb, key);
		}
		if (node == null) {
			node = factory.instantiate(owner, key, options);
			node.setState(false,  writableNotReadable, !writableNotReadable);
			owner.put(key, node);
		}

		return node;
	}

	/**
	 * Return this node with status as though it had been closed.
	 * Throw an exception if the node is not logically open.
	 */
	public Node getForClose() {
		
		if (!isOpen()) {
			throwRuntimeException(getTypeName(), "not open", "close", key);
		}
		setState(false, isWritable(), !isReadable());

		return this;
	}

	private static void throwRuntimeException(String entity, String state, String verb, Object key) {
		throw new RuntimeException(entity + " is " + state + "; cannot " + verb + ": " + key.toString());
	}
}
