/*
java-statsd-client. Copyright (C) 2014 youDevise, Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package com.hauldata.dbpa.manage.statsd;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Pattern;

public class DummyStatsDServer {
	private final List<String> messagesReceived = new LinkedList<String>();
	private final DatagramSocket server;

	public DummyStatsDServer(int port) {
		try {
			server = new DatagramSocket(port);
		} catch (SocketException e) {
			throw new IllegalStateException(e);
		}
		new Thread(new Runnable() {
			@Override public void run() {
				try {
					while (true) {
						final DatagramPacket packet = new DatagramPacket(new byte[256], 256);
						server.receive(packet);
						synchronized (messagesReceived) {
							messagesReceived.add(new String(packet.getData(), Charset.forName("UTF-8")).trim());
						}
					}
				}
				catch (Exception e) {}
			}
		}).start();
	}

	public void stop() {
	    server.close();
	}

	public boolean waitForMessageLike(String regex, long ms, boolean remove) {
		final long messageCheckMs = 100L;

		Pattern pattern = Pattern.compile(regex);
		Instant startInstant = Instant.now();
		Instant endInstant = startInstant.plus(ms, ChronoUnit.MILLIS);

		ListIterator<String> messageIterator = messagesReceived.listIterator();
		try {
			while (Instant.now().isBefore(endInstant)) {
				synchronized (messagesReceived) {
					while (messageIterator.hasNext()) {
						String message = messageIterator.next();
						if (pattern.matcher(message).find()) {
							if (remove) {
								messageIterator.remove();
							}
						return true;
						}
					}
				}
				Thread.sleep(messageCheckMs);
			}
		}
		catch (InterruptedException e) {}
		return false;
    }
}
