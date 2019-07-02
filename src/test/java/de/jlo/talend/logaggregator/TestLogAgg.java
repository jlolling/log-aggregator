package de.jlo.talend.logaggregator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestLogAgg {

	@Test
	public void testSeparateMessageByTimeBetweenLines() throws Exception {
		long maxTimeBetweenLinesOfAMessage = 10l;
		PipeLogger l = new PipeLogger();
		l.setJobName("test_job1");
		l.initLog4J();
		l.setAddStartStopMessage(true);
		TestAppender ta = new TestAppender();
		ta.setDebug(true);
		l.getLogger().addAppender(ta);
		l.setMaxTimeBetweenLinesOfAMessage(maxTimeBetweenLinesOfAMessage);
		l.startWriter();
		for (int i = 0; i < 20; i++) {
			l.log("i=" + i);
			if (i > 0 && i % 10 == 0) {
				Thread.sleep(maxTimeBetweenLinesOfAMessage + (maxTimeBetweenLinesOfAMessage / 2));
			}
		}
		l.stop();
		l.waitUntilEnd();
		assertEquals(4, ta.getCountEvents());
	}

	@Test
	public void testSeparateMessageByTimeToKeep() throws Exception {
		long maxTimeBetweenLinesOfAMessage = 100l;
		PipeLogger l = new PipeLogger();
		l.setJobName("test_job1");
		l.initLog4J();
		l.setAddStartStopMessage(true);
		TestAppender ta = new TestAppender();
		ta.setDebug(true);
		l.getLogger().addAppender(ta);
		l.setMaxTimeBetweenLinesOfAMessage(maxTimeBetweenLinesOfAMessage);
		l.setMaxTimeToKeepAMessage(1000l);
		l.startWriter();
		for (int i = 0; i < 20; i++) {
			l.log("i=" + i);
			Thread.sleep(90l);
		}
		l.stop();
		l.waitUntilEnd();
		assertEquals(4, ta.getCountEvents());
	}

	@Test
	public void testSeparateMessageBySignal() throws Exception {
		long maxTimeBetweenLinesOfAMessage = 10l;
		PipeLogger l = new PipeLogger();
		l.setJobName("test_job2");
		l.initLog4J();
		l.setAddStartStopMessage(true);
		TestAppender ta = new TestAppender();
		ta.setDebug(true);
		l.getLogger().addAppender(ta);
		l.setMaxTimeBetweenLinesOfAMessage(maxTimeBetweenLinesOfAMessage);
		l.startWriter();
		for (int i = 0; i < 20; i++) {
			l.log("i=" + i);
			if (i > 0 && i % 10 == 0) {
				l.log(l.getFlushMessageSignal());
			}
		}
		l.stop();
		l.waitUntilEnd();
		assertEquals(4, ta.getCountEvents());
	}

	@Test
	public void testSeparateMessageBySize() throws Exception {
		long maxTimeBetweenLinesOfAMessage = 10l;
		PipeLogger l = new PipeLogger();
		l.setJobName("test_job3");
		l.initLog4J();
		l.setAddStartStopMessage(true);
		TestAppender ta = new TestAppender();
		ta.setDebug(true);
		l.getLogger().addAppender(ta);
		l.setMaxTimeBetweenLinesOfAMessage(maxTimeBetweenLinesOfAMessage);
		l.setMaxMessageSize(50);
		l.startWriter();
		for (int i = 0; i < 20; i++) {
			l.log("i=" + i);
		}
		l.stop();
		l.waitUntilEnd();
		assertEquals(4, ta.getCountEvents());
	}

}
