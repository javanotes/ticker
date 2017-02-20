package ticker.actor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivetechnologies.ticker.Ticker;
import org.reactivetechnologies.ticker.messaging.actors.MessagingContainerSupport;
import org.reactivetechnologies.ticker.messaging.base.Publisher;
import org.reactivetechnologies.ticker.messaging.base.ringbuff.RingBufferedQueueContainer;
import org.reactivetechnologies.ticker.messaging.data.TextData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @deprecated {@linkplain RingBufferedQueueContainer} is experimental.
 * @author esutdal
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {Ticker.class})
public class QueueListenerDisruptorTest {
	
	static final int NO_OF_ITEMS = SimpleQueueListener.NO_OF_MESSAGES;
	static final int PUB_THREADS = 4;
	
	@Autowired
	Publisher pub;
	
	@Autowired
	MessagingContainerSupport containerSupport;
	
	@Before
	public void pre()
	{
		testAddToQueue();
	}
	
	public void testAddToQueue()
	{
		
		System.err.println("Starting testAddToQueue........");
		System.err.println("Iterations => "+NO_OF_ITEMS);
		System.err.println("Threads => "+PUB_THREADS);
		ExecutorService ex = Executors.newFixedThreadPool(PUB_THREADS);
		long t = System.currentTimeMillis();
		AtomicInteger i=new AtomicInteger();
		for(int j=0; j<PUB_THREADS; j++)
		{
			ex.submit(new Runnable() {
				
				@Override
				public void run() 
				{
					int idx = i.getAndIncrement();
					do {
						
						try 
						{
							pub.offer(new TextData("HELLOCMQ " + idx, SimpleQueueListener.QNAME));
							//Assert.assertTrue(resp);
						} catch (Exception e) {
							e.printStackTrace();
							Assert.fail();
						} 
						idx = i.getAndIncrement();
					}
					while(idx < NO_OF_ITEMS);
				}
			});
			
		}
		boolean await = false;
		ex.shutdown();
		try {
			await = ex.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			
		}
		System.err.println("End run... Time taken in millis: "+(System.currentTimeMillis()-t));
		Assert.assertTrue(await);
		
	}
	
	
	static final Logger log = LoggerFactory.getLogger(QueueListenerDisruptorTest.class);
	@Test
	public void pollFromQueue() throws TimeoutException
	{
		CountDownLatch l = new CountDownLatch(NO_OF_ITEMS);
		long start = System.currentTimeMillis();
		containerSupport.registerListener(new SimpleQueueListener(l));
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
