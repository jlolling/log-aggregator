package de.jlo.talend.logaggregator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

import org.graylog2.GelfMessage;
import org.graylog2.GelfSenderResult;
import org.graylog2.GelfTCPSender;

public class GelfTCPSenderExt extends GelfTCPSender {

	private boolean shutdown = false;
	private InetAddress host;
	private int port;
	private Socket socket;
    private OutputStream os;
    private int timeout = 0;
    private int waitTimeAfterFailure = 500;
    private int maxRetry = 5;

    public GelfTCPSenderExt() {}

	public GelfTCPSenderExt(String host, int port, int timeout, int maxRetry) throws IOException {
		this.timeout = timeout;
		this.maxRetry = maxRetry;
		this.host = InetAddress.getByName(host);
		this.port = port;
	}
	
	private void setupSocket() throws IOException {
		if (socket == null || os == null) {
			this.socket = new Socket(host, port);
			this.socket.setSoTimeout(timeout);
	        this.os = socket.getOutputStream();
		}
	}

	@Override
	public GelfSenderResult sendMessage(GelfMessage message) {
		if (shutdown || message.isValid() == false) {
			return GelfSenderResult.MESSAGE_NOT_VALID_OR_SHUTTING_DOWN;
		}
		Exception ex = null;
		int currentAttempt = 0;
		boolean ok = false;
		byte[] data = message.toTCPBuffer().array();
		for (currentAttempt = 0; currentAttempt <= maxRetry; currentAttempt++) {
			try {
				// reconnect if necessary
				setupSocket();
				os.write(data);
				ok = true;
				break;
			} catch (IOException e) {
				ex = e;
				socket = null;
				if (os != null) {
					try {
						os.close();
					} catch (Throwable ie) {
						// ignore
					}
					os = null;
				}
				if (currentAttempt <= maxRetry) {
					try {
						Thread.sleep(waitTimeAfterFailure);
					} catch (Exception ie) {
						System.err.println("Send message per TCP failed. (Will retry: attempt: #" + currentAttempt + " / " + maxRetry + ") " + ex.getMessage());
						return new GelfSenderResult(GelfSenderResult.ERROR_CODE, e);
					}
				}
			}
		}
		if (ok) {
			return GelfSenderResult.OK;
		} else {
			System.err.println("Send message per TCP failed (No retry left. attempt: #" + currentAttempt + " / " + maxRetry + "). " + ex.getMessage());
			return new GelfSenderResult(GelfSenderResult.ERROR_CODE, ex);
		}
	}

	@Override
	public void close() {
		shutdown = true;
		try {
            if (os != null) {
                os.close();
            }
			if (socket != null) {
				socket.close();
			}
		} catch (IOException e) {
			// ignore
		}
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getMaxRetry() {
		return maxRetry;
	}

	public void setMaxRetry(int maxRetry) {
		this.maxRetry = maxRetry;
	}
	
}
