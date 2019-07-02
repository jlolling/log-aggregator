package de.jlo.talend.logaggregator;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Log line aggregator
 * @author jan.lolling@gmail.com
 *
 */
public class PipeLogger {
	
	private String log4jConfigFile = null;
	private ReadPipeThread reader = null;
	private AggregateAndWriteLogsThread writer = null;
	private int queueSize = 10000;
	private BlockingDeque<String> dequeue = new LinkedBlockingDeque<String>(queueSize);
	private long maxTimeBetweenLinesOfAMessage = 500l;
	private long maxTimeToKeepAMessage = 5000l;
	private int maxMessageSize = 30720;
	private int incomingBufferSize = 16348;
	private static final String THE_END = "STOP_LOGGING";
	private String flushMessageSignal = "FLUSH_MESSAGE";
	private Logger logger = null;
	private Exception readerException = null;
	private Exception writerException = null;
	private String jobName = null;
	private String jobVersion = null;
	private String graylogHost = null;
	private Date applicationStartTime = new Date();
	private boolean addStartStopMessage = true;
	private String layer = null;
	private String originHost = null;
	private Map<String, String> additionalFieldMap = new HashMap<>();
	private int timeout = 5000;
	private boolean sendStandardOutput = false;
	
    public static void main(String[] args) {
    	PipeLogger la = new PipeLogger();
    	la.configure(args);
    	try {
        	la.initLog4J();
    	} catch (Exception e) {
    		e.printStackTrace();
    		System.exit(3);
    	}
    	la.start();
    	la.waitUntilEnd();
    	la.exit();
    }
    
    public void configure(String[] args) {
    	Options options = new Options();
    	options.addOption("j", "job_name", true, "Job name");
    	options.addOption("t", "application_name", true, "Job name (compatible to logger)");
    	options.addOption("v", "application_version", true, "Job Version");
    	options.addOption("c", "config_file", true, "Log4j config file");
    	options.addOption("g", "graylog_host", true, "Graylog host [host|host:port|tcp:host|tcp:host:port]");
    	options.addOption("o", "graylog_timeout", true, "Timeout for Graylog TCP sender (default=5000)");
    	options.addOption("q", "queue_size", true, "Message queue size");
    	options.addOption("s", "max_message_size", true, "Max message size");
    	options.addOption("x", "max_time_between_lines", true, "Max time between lines to get them as one message");
    	options.addOption("y", "max_time_until_send", true, "Max time to collect data until a new message will be send");
    	options.addOption("p", "pid", true, "Process identifier");
    	options.addOption("a", "add_start_stop", true, "Add a message for start and stop (default = true)");
    	options.addOption("l", "layer", true, "Layer or system stage: [test|ref|prod]");
    	options.addOption("r", "redirect_to_stndout", true, "Redirect all input received via pipe also to standard out. (default=false)");
    	options.addOption("f", "flush_message_signal", true, "Text to force flush the message. (default=FLUSH_MESSAGE)");
    	CommandLineParser parser = new DefaultParser();
    	try {
	    	String processInfo = ManagementFactory.getRuntimeMXBean().getName();
			int p = processInfo.indexOf('@');
			if (p > 0) {
				originHost = processInfo.substring(p + 1);
			} else {
				originHost = processInfo;
			}
			String message = null;
    		CommandLine cmd = parser.parse( options, args);
			jobName = cmd.getOptionValue('j');
			if (jobName == null) {
				jobName = cmd.getOptionValue('t');
			}
			if (jobName == null) {
				message = "Parameter jobname or application_name must be set.";
			}
			jobVersion = cmd.getOptionValue('v');
			log4jConfigFile = cmd.getOptionValue('c');
			String mtbl_string = cmd.getOptionValue('x');
			try {
				if (mtbl_string != null && mtbl_string.trim().isEmpty() == false) {
					setMaxTimeBetweenLinesOfAMessage(Long.valueOf(mtbl_string));
				}
			} catch (Exception e1) {
				message = "Parameter x must be an long value.";
			}
			String mtuns_string = cmd.getOptionValue('y');
			try {
				if (mtuns_string != null && mtuns_string.trim().isEmpty() == false) {
					setMaxTimeToKeepAMessage(Long.valueOf(mtuns_string));
				}
			} catch (Exception e1) {
				message = "Parameter y must be an long value.";
			}
			String size = cmd.getOptionValue('s');
			try {
				if (size != null && size.trim().isEmpty() == false) {
					setMaxMessageSize(Integer.valueOf(size));
				}
			} catch (Exception e1) {
				message = "Parameter s must be an integer value.";
			}
			if (message != null) {
				System.out.println(message);
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("java -jar log-aggregator-<version>.jar", options);
				System.exit(4);
			}
			graylogHost = cmd.getOptionValue('g');
			String a = cmd.getOptionValue('a');
			if (a != null && a.trim().isEmpty() == false) {
				addStartStopMessage = ("false".equalsIgnoreCase(a) == false);
			}
			layer = cmd.getOptionValue('l');
			if (layer == null) {
				// try to detect the layer by server name
				if (originHost.contains("test")) {
					layer = "test";
				} else if (originHost.contains("ref")) {
					layer = "ref";
				} else if (originHost.contains("talendjob")) {
					layer = "prod";
				}
			}
			String timeoutStr = cmd.getOptionValue('o');
			if (timeoutStr != null && timeoutStr.trim().isEmpty() == false) {
				try {
					timeout = Integer.parseInt(timeoutStr);
					if (timeout <= 0) {
						throw new Exception("Graylog timeout must be an integer value > 0");
					}
				} catch (Exception pe) {
					message = "Get Graylog timeout failed. Error: " + pe.getMessage();
				}
			}
			sendStandardOutput = "true".equalsIgnoreCase(cmd.getOptionValue('r'));
		} catch (Exception e) {
			System.err.println(e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar log-aggregator-<version>.jar", options);
			System.exit(4);
		}
    }
    
    public void exit() {
    	LogManager.shutdown();
    	if (readerException != null) {
    		System.exit(1);
    	} else if (writerException != null) {
    		System.exit(2);
    	} else {
        	System.exit(0);
    	}
    }
    
    public void initLog4J() throws Exception {
    	if (jobName == null) {
    		throw new Exception("Job name not set!");
    	}
		String jobStartedAt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(applicationStartTime);
    	MDC.put("application_name", jobName);
    	MDC.put("job_started_at", jobStartedAt);
    	logger = Logger.getLogger(jobName);
    	boolean configured = false;
		if (log4jConfigFile != null && log4jConfigFile.trim().isEmpty() == false) {
			try {
				// find the log4j configuration and configure it
				File cf = new File(log4jConfigFile);
				if (cf.exists()) {
					if (log4jConfigFile.endsWith(".xml")) {
						DOMConfigurator.configureAndWatch(cf.getAbsolutePath(), 10000);
						configured = true;
					} else if (log4jConfigFile.endsWith(".properties")) {
						PropertyConfigurator.configureAndWatch(cf.getAbsolutePath(), 10000);
						configured = true;
					} else {
						throw new Exception("Unknown log4j file format:" + log4jConfigFile);
					}
				} else {
					throw new Exception("Log4j config file: " + cf.getAbsolutePath() + " does not exist.");
				}
			} catch (Throwable e) {
				throw new Exception(e);
			}
		}
		if (graylogHost != null && graylogHost.trim().isEmpty() == false) {
			GelfAppenderExt ga = new GelfAppenderExt();
			boolean useTcpForGraylog = graylogHost.startsWith("tcp:");
			if (useTcpForGraylog) {
				graylogHost = graylogHost.substring("tcp:".length());
			}
			int pos = graylogHost.indexOf(":");
			if (pos > 0) {
				ga.setGraylogHost((useTcpForGraylog ? "tcp:" : "") + graylogHost.substring(0, pos));
				ga.setGraylogPort(Integer.parseInt(graylogHost.substring(pos + 1)));
			} else {
 				ga.setGraylogHost((useTcpForGraylog ? "tcp:" : "") + graylogHost);
			}
			additionalFieldMap.put("application_name", jobName);
			additionalFieldMap.put("application_version", jobVersion);
			additionalFieldMap.put("job_started_at", jobStartedAt);
			additionalFieldMap.put("system_stage", layer);
			ga.setAdditionalFields(buildAdditionalFields());
			ga.setOriginHost(originHost);
			ga.setAddExtendedInformation(true);
			ga.setIncludeLocation(true);
			ga.setLayout(new PatternLayout());
			ga.setExtractStacktrace(true);
			ga.setFacility("talend-job");
			ga.activateOptions();
			logger.addAppender(ga);
			configured = true;
		}
		if (configured == false) {
			BasicConfigurator.configure();
		}
    }

    private void checkLogger() {
    	if (logger == null) {
    		throw new IllegalStateException("Logger not initialized. Please call initLog4J() before.");
    	}
    }
    
    public Logger getLogger() {
    	checkLogger();
    	return logger;
    }
    
    private String buildAdditionalFields() {
    	StringBuilder sb = new StringBuilder();
    	boolean firstLoop = true;
    	for (Map.Entry<String, String> entry : additionalFieldMap.entrySet()) {
    		if (firstLoop) {
    			firstLoop = false;
    			sb.append("{");
    		} else {
    			sb.append(",");
    		}
    		if (entry.getValue() != null) {
        		sb.append("'");
        		sb.append(entry.getKey());
        		sb.append("':'");
        		sb.append(entry.getValue());
        		sb.append("'");
    		}
    	}
    	if (firstLoop == false) {
			sb.append("}");
    	}
    	if (sb.length() > 0) {
    		return sb.toString();
    	} else {
    		return null;
    	}
    }
    
    public void waitUntilEnd() {
    	if (writer != null) {
        	try {
    			writer.join();
    		} catch (InterruptedException e) {
    			// ignore
    		}
    	}
    	if (reader != null) {
        	try {
    			reader.join();
    		} catch (InterruptedException e) {
    			// ignore
    		}
    	}
    }
    
    public void start() {
    	startWriter();
    	startPipeReader();
    }
    
    public void stop() {
    	if (reader != null) {
    		reader.stopReading();
    	}
    	try {
        	dequeue.put(THE_END);
    	} catch (Exception e) {
    		//ignore
    	}
    }
    
    public void startPipeReader() {
    	reader = new ReadPipeThread();
    	reader.setDaemon(true);
    	reader.setName("reader-thread#" + reader.hashCode());
    	reader.start();
    }
    
    public void startWriter() {
    	writer = new AggregateAndWriteLogsThread();
    	writer.setDaemon(true);
    	writer.setName("writer-thread#" + writer.hashCode());
    	writer.start();
    	if (addStartStopMessage) {
    		checkLogger();
    		logger.info("Start: " + jobName);
    	}
    }
    
    private class ReadPipeThread extends Thread {
    	
    	private boolean stop = false;
    	
    	public void stopReading() {
    		stop = true;
    		try {
    			//System.out.println("put: " + THE_END);
        		dequeue.put(THE_END);
    		} catch (Exception e) {
    			// ignore
    		}
    	}
    	
    	@Override
    	public void run() {
    		try {
    	    	BufferedReader in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"), incomingBufferSize);
    			if (in != null) {
    				String line = null;
    				while (stop == false) {
    					line = in.readLine();
    					if (line != null) {
    						//System.out.println("put: " + line);
        					dequeue.put(line);
        					if (sendStandardOutput) {
        						System.out.println(line);
        					}
    					} else {
    						break;
    					}
    				}
					//System.out.println("put: " + THE_END);
					dequeue.put(THE_END);
    				in.close();
    			}
    		} catch (Exception e) {
    			System.err.println("Read from standard in failed: " + e.getMessage());
    			e.printStackTrace();
    			readerException = e;
    		} finally {
    			try {
        			dequeue.put(THE_END);
    			} catch (Exception e2) {
    				// ignore
    			}
    		}
    	}
    	
    }
    
    public void log(String line) throws Exception {
    	if (line != null && line.trim().isEmpty() == false) {
			//System.out.println("put: " + line);
    		dequeue.put(line);    	
    	}
    }
    
    private class AggregateAndWriteLogsThread extends Thread {
    	
    	@Override
    	public void run() {
    		if (logger == null) {
    			writerException = new Exception("Log4j not configured");
    			writerException.printStackTrace();
    			return;
    		}
    		StringBuilder message = new StringBuilder(1000);
			long lastMessageSendAt = System.currentTimeMillis();
			boolean stop = false;
			while (stop == false) {
				if (Thread.currentThread().isInterrupted()) {
					stop = true;
					break;
				}
				try {
					String line = dequeue.poll(maxTimeBetweenLinesOfAMessage, TimeUnit.MILLISECONDS);
					//System.out.println("poll: " + line);
					boolean sendMessage = false;
					if (line != null) {
						if (line.equals(THE_END)) {
							//System.out.println(THE_END + " received");
							stop = true;
							sendMessage = true;
						} else if (line.equals(flushMessageSignal)) {
							sendMessage = true;
						} else {
							long currentMessageReceivedAt = System.currentTimeMillis();
							// input within time frame for a message
							message.append(line);
							message.append("\n");
							if (message.length() > maxMessageSize) {
								sendMessage = true;
							} else if (maxTimeToKeepAMessage > 0l && (currentMessageReceivedAt - lastMessageSendAt) > maxTimeToKeepAMessage) {
								sendMessage = true;
							} else {
								sendMessage = false;
							}
						}
					} else {
						sendMessage = true;
					}
					if (sendMessage) {
						if (message.length() > 0) {
							// input outside time frame of a message
							// send input to the logger
							String s = message.toString();
							if (s.contains("Error") || s.contains("Exception")) {
								logger.error(s);
							} else {
								logger.info(s);
							}
							message.setLength(0);
							lastMessageSendAt = System.currentTimeMillis();
						}
					}
					if (stop) {
						break;
					}
				} catch (InterruptedException ie) {
					stop = true;
					break;
				} catch (Exception e) {
					stop = true;
					System.err.println("Aggregate and send log messages failed: " + e.getMessage());
					e.printStackTrace();
					writerException = e;
					if (reader != null) {
						reader.stopReading();
					}
				}
			}
	    	if (addStartStopMessage) {
	    		logger.info("Stop: " + jobName);
	    	}
	    	LogManager.shutdown();
    	}
    	
    }

	public long getMaxTimeBetweenLinesOfAMessage() {
		return maxTimeBetweenLinesOfAMessage;
	}


	public void setMaxTimeBetweenLinesOfAMessage(long maxTimeBetweenLinesOfAMessage) {
		this.maxTimeBetweenLinesOfAMessage = maxTimeBetweenLinesOfAMessage;
	}

	public String getLog4jConfigFile() {
		return log4jConfigFile;
	}

	public void setLog4jConfigFile(String log4jConfigFile) {
		this.log4jConfigFile = log4jConfigFile;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getGraylogHost() {
		return graylogHost;
	}

	public void setGraylogHost(String graylogHost) {
		this.graylogHost = graylogHost;
	}

	public long getMaxTimeToKeepAMessage() {
		return maxTimeToKeepAMessage;
	}

	public void setMaxTimeToKeepAMessage(long maxTimeToKeepAMessage) {
		this.maxTimeToKeepAMessage = maxTimeToKeepAMessage;
	}

	public int getMaxMessageSize() {
		return maxMessageSize;
	}

	public void setMaxMessageSize(int maxMessageSize) {
		this.maxMessageSize = maxMessageSize;
	}

	public String getLayer() {
		return layer;
	}

	public void setLayer(String systemStage) {
		this.layer = systemStage;
	}

	public String getJobVersion() {
		return jobVersion;
	}

	public void setJobVersion(String jobVersion) {
		this.jobVersion = jobVersion;
	}

	public boolean isAddStartStopMessage() {
		return addStartStopMessage;
	}

	public void setAddStartStopMessage(boolean addStartStopMessage) {
		this.addStartStopMessage = addStartStopMessage;
	}

	public String getFlushMessageSignal() {
		return flushMessageSignal;
	}

	public void setFlushMessageSignal(String flushMessageSignal) {
		if (flushMessageSignal != null && flushMessageSignal.trim().isEmpty() == false) {
			this.flushMessageSignal = flushMessageSignal;
		}
	}
    
}
