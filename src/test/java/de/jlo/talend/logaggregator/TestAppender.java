package de.jlo.talend.logaggregator;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class TestAppender extends AppenderSkeleton {
	
	private List<LoggingEvent> listEvents = new ArrayList<LoggingEvent>();
	private boolean debug = false;

	@Override
	public void close() {
	}

	@Override
	public boolean requiresLayout() {
		return false;
	}

	@Override
	protected void append(LoggingEvent event) {
		listEvents.add(event);
		if (debug) {
			System.out.println("#" + listEvents.size() + "[" + event.getThreadName() + "]" + " > " + event.getLoggerName() + "@" + event.getTimeStamp() + "=\n>>>>\n" + event.getMessage() + "\n<<<<\n");
		}
	}
	
	public int getCountEvents() {
		return listEvents.size();
	}
	
	public List<LoggingEvent> getListEvents() {
		return listEvents;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

}
