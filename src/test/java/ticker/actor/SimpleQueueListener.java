package ticker.actor;

import java.util.concurrent.CountDownLatch;

import org.reactivetechnologies.ticker.messaging.base.AbstractQueueListener;
import org.reactivetechnologies.ticker.messaging.data.TextData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleQueueListener extends AbstractQueueListener<TextData> {
	public SimpleQueueListener() {
	}
	public SimpleQueueListener(CountDownLatch latch) {
		this.latch = latch;
	}

	private CountDownLatch latch;
	public static final String QNAME = "TEST-QUEUE";
	public static final int NO_OF_MESSAGES = 100;
	
	static final Logger log = LoggerFactory.getLogger(SimpleQueueListener.class);

	@Override
	public Class<TextData> dataType() {
		return TextData.class;
	}

	@Override
	public void onMessage(TextData m) throws Exception {
		log.info("Recieved message ... " + m.getPayload());
		if(latch != null)
			latch.countDown();
	}

	public int parallelism() {
		return 20;
	}

	@Override
	public String routing() {
		return QNAME;
	}

	@Override
	public void destroy() {
		log.info("--- SimpleQueueListener.destroy() ---");
	}

	@Override
	public void init() {
		log.info("--- SimpleQueueListener.init() ---");
	}

}
