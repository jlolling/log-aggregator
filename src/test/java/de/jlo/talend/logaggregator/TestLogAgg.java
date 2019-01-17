package de.jlo.talend.logaggregator;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import de.jlo.talend.logaggregator.LogAgg;

public class TestLogAgg {

	@Test
	public void testLineCombining() throws Exception {
		long maxTimeBetweenLinesOfAMessage = 10l;
		LogAgg l = new LogAgg();
		l.setJobName("test_job");
		l.initLog4J();
		l.setMaxTimeBetweenLinesOfAMessage(maxTimeBetweenLinesOfAMessage);
		l.startWriter();
		for (int i = 0; i < 10000; i++) {
			l.log(i + "");
			if (i % 100 == 0) {
				Thread.sleep(maxTimeBetweenLinesOfAMessage + (maxTimeBetweenLinesOfAMessage / 2));
			}
		}
		l.stop();
		assertTrue(true);
	}

}
