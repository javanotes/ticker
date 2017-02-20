package ticker.actor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivetechnologies.ticker.Ticker;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {Ticker.class})
public class QueueRestIngestionTest {

	static final String INGEST_URL = "http://localhost:8081/ticker/append/"+SimpleQueueListener.QNAME;
	
	TestRestTemplate rest = new TestRestTemplate();
	
	private final int iteration = 100;
	private final int connThreads = 1;
	
	@Before
	public void pre()
	{
		
	}
	@Test
	public void testAddToQueue()
	{
		
		System.err.println("Starting test........");
		System.err.println("Iterations => "+iteration);
		System.err.println("Threads => "+connThreads);
		ExecutorService ex = Executors.newFixedThreadPool(connThreads);
		long t = System.currentTimeMillis();
		AtomicInteger i=new AtomicInteger();
		for(int j=0; j<connThreads; j++)
		{
			ex.submit(new Runnable() {
				
				@Override
				public void run() 
				{
					int idx = i.getAndIncrement();
					do {
						
						try {
							@SuppressWarnings("rawtypes")
							ResponseEntity<HttpEntity> resp = rest.postForEntity(INGEST_URL,
									"HELLOCMQ " + idx, HttpEntity.class);
							Assert.assertEquals(201, resp.getStatusCodeValue());
						} catch (Exception e) {
							e.printStackTrace();
							Assert.fail();
						} 
						idx = i.getAndIncrement();
					}
					while(idx < iteration);
				}
			});
			
		}
		boolean await = false;
		ex.shutdown();
		try {
			await = ex.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			
		}
		
		Assert.assertTrue(await);
		System.err.println("End run... Time taken in millis: "+(System.currentTimeMillis()-t));
		
		/*Assert.assertEquals(iteration, metrics.getEnqueueCount(SimpleQueueListener.QNAME));
		Assert.assertEquals(0, metrics.getDequeueCount(SimpleQueueListener.QNAME));
		Assert.assertEquals(iteration, service.size(SimpleQueueListener.QNAME));*/
	}
	
}
