package de.jlo.talend.logaggregator;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestLogAgg {

	@Test
	public void testLineCombining() throws Exception {
		long maxTimeBetweenLinesOfAMessage = 10l;
		PipeLogger l = new PipeLogger();
		l.setJobName("test_job");
		l.initLog4J();
		l.setMaxTimeBetweenLinesOfAMessage(maxTimeBetweenLinesOfAMessage);
		//l.setMaxMessageSize(20);
		l.startWriter();
		for (int i = 0; i < 2; i++) {
			l.log(i + "");
			if (i % 10 == 0) {
				Thread.sleep(maxTimeBetweenLinesOfAMessage + (maxTimeBetweenLinesOfAMessage / 2));
			}
		}
		l.stop();
		l.waitUntilEnd();
		assertTrue(true);
	}

}
