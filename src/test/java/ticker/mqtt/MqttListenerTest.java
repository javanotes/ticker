package ticker.mqtt;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivetechnologies.ticker.Ticker;
import org.reactivetechnologies.ticker.messaging.actors.MessagingContainerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import akka.actor.Inbox;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {Ticker.class})
public class MqttListenerTest {

	public static final int NO_OF_ITEMS = 100000;
			
	@Autowired
	Inbox inbox;
	
	@Autowired
	MessagingContainerSupport containerSupport;
	
	static final Logger log = LoggerFactory.getLogger(MqttListenerTest.class);
	@Test
	public void pollFromQueue() throws TimeoutException
	{
		CountDownLatch l = new CountDownLatch(NO_OF_ITEMS);
		containerSupport.registerListener(new Listener(l));
		long start = System.currentTimeMillis();
		containerSupport.start();
		
		log.info("Started container run.........................");
		try {
			boolean b = l.await(300, TimeUnit.SECONDS);
			Assert.assertTrue(b);
		} catch (InterruptedException e) {
			
		}
		
		long time = System.currentTimeMillis() - start;
		long secs = TimeUnit.MILLISECONDS.toSeconds(time);
		log.info("Time taken: " + secs + " secs " + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
	}
}
