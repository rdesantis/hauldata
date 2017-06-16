package com.hauldata.dbpa;

public class QuietLog4j {

	static private boolean isQuiet = false;
	public static void please() {
		if (!isQuiet) {
			org.apache.log4j.BasicConfigurator.configure(new org.apache.log4j.varia.NullAppender());
			isQuiet = true;
		}
	}
}
