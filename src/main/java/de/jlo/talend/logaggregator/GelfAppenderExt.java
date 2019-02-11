package de.jlo.talend.logaggregator;

import java.io.IOException;

import org.graylog2.GelfTCPSender;
import org.graylog2.log.GelfAppender;

public class GelfAppenderExt extends GelfAppender {
	
	private int timeout = 5000;
	private int retry = 5;

	@Override
	protected GelfTCPSender getGelfTCPSender(String tcpGraylogHost, int graylogPort) throws IOException {
        return new GelfTCPSenderExt(tcpGraylogHost, graylogPort, timeout, retry);
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getRetry() {
		return retry;
	}

	public void setRetry(int retry) {
		this.retry = retry;
	}

}
