package ticker.mqtt;

import java.util.concurrent.CountDownLatch;

import org.reactivetechnologies.ticker.messaging.base.AbstractQueueListener;
import org.reactivetechnologies.ticker.mqtt.MqttData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Listener extends AbstractQueueListener<MqttData> {
	public Listener() {
	}
	public Listener(CountDownLatch latch) {
		this.latch = latch;
	}

	private CountDownLatch latch;
	static final Logger log = LoggerFactory.getLogger(Listener.class);

	@Override
	public Class<MqttData> dataType() {
		return MqttData.class;
	}

	@Override
	public void onMessage(MqttData m) throws Exception {
		//log.info("Recieved message ... " + m.toUtf8());
		if(latch != null)
			latch.countDown();
	}

	public int parallelism() {
		return 20;
	}

	@Override
	public String routing() {
		return Publisher.TOPIC_TEMPERATURE;
	}
	
}
