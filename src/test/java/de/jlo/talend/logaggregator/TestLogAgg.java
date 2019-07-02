package de.jlo.talend.logaggregator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestLogAgg {

	@Test
	public void testLineCombining() throws Exception {
		long maxTimeBetweenLinesOfAMessage = 10l;
		PipeLogger l = new PipeLogger();
		l.setJobName("test_job");
		l.initLog4J();
		l.setAddStartStopMessage(true);
		TestAppender ta = new TestAppender();
		ta.setDebug(true);
		l.getLogger().addAppender(ta);
		l.setMaxTimeBetweenLinesOfAMessage(maxTimeBetweenLinesOfAMessage);
		//l.setMaxMessageSize(20);
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
	public void testSeparateMessageBySignal() throws Exception {
		long maxTimeBetweenLinesOfAMessage = 10l;
		PipeLogger l = new PipeLogger();
		l.setJobName("test_job");
		l.initLog4J();
		l.setAddStartStopMessage(true);
		TestAppender ta = new TestAppender();
		l.getLogger().addAppender(ta);
		l.setMaxTimeBetweenLinesOfAMessage(maxTimeBetweenLinesOfAMessage);
		//l.setMaxMessageSize(20);
		l.startWriter();
		for (int i = 0; i < 20; i++) {
			l.log(i + "");
			if (i > 0 && i % 10 == 0) {
				//l.log(l.getFlushMessageSignal());
			}
		}
		l.stop();
		l.waitUntilEnd();
		assertEquals(4, ta.getCountEvents());
	}

}
