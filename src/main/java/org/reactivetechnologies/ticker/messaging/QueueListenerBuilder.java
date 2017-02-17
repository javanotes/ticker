package org.reactivetechnologies.ticker.messaging;

import org.reactivetechnologies.ticker.messaging.base.AbstractQueueListener;
import org.reactivetechnologies.ticker.messaging.base.Consumer;
import org.reactivetechnologies.ticker.messaging.base.QueueListener;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;
/**
 * Builder class for fluently creating {@linkplain QueueListener} instances.
 * @author esutdal
 *
 */
@ConfigurationProperties(prefix = "consumer.config")
public class QueueListenerBuilder {

	private int concurrency = -1;
	private String route;
	private String identifier;
	private Class<? extends Data> data;
	private Consumer<? extends Data> consumer;
	private boolean useSharedPool = true;
	
	public QueueListenerBuilder() {
	}
	public QueueListenerBuilder identifier(String c)
	{
		this.identifier = c;
		return this;
	}
	public QueueListenerBuilder concurrency(int c)
	{
		this.concurrency = c;
		return this;
	}
	
	public QueueListenerBuilder route(String r)
	{
		this.route = r;
		return this;
	}
	public QueueListenerBuilder sharedPool(boolean r)
	{
		this.useSharedPool = r;
		return this;
	}
	
	public <T extends Data> QueueListenerBuilder dataType(Class<T> d)
	{
		this.data = d;
		return this;
	}
	public <T extends Data> QueueListenerBuilder consumer(Consumer<T> d)
	{
		this.consumer = d;
		return this;
	}
	private class DefaultQueueListener<T extends Data> extends AbstractQueueListener<T> {
		
		private Consumer<T> consumer;
		private Class<T> data;
		
		public void setData(Class<T> data) {
			this.data = data;
		}

		public DefaultQueueListener() {
		}
				
		@Override
		public Class<T> dataType() {
			return data;
		}

		@Override
		public boolean useSharedPool()
		{
			return useSharedPool;
			
		}
		
		public String identifier() {
			return identifier != null ? identifier : super.identifier();
		}
		
		public int parallelism() {
			return concurrency != -1 ? concurrency : super.parallelism();
		}
		
		@Override
		public void onMessage(T m) throws Exception {
			this.consumer.onMessage(m);
		}

		@Override
		public String routing() {
			return route;
		}

		public void setConsumer(Consumer<T> consumer2) {
			this.consumer = consumer2;
		}

		@Override
		public void init() {
			this.consumer.init();
		}

	}
	@SuppressWarnings("unchecked")
	public <T extends Data> QueueListener<T> build()
	{
		Assert.notNull(data, "dataType() is reqd for QueueListener");
		Assert.notNull(route, "routing() is reqd for QueueListener");
		Assert.notNull(consumer, "consumer() is reqd for QueueListener");
		
		DefaultQueueListener<T> qListener = new DefaultQueueListener<>();
		qListener.setConsumer((Consumer<T>) consumer);
		qListener.setData((Class<T>) data);
		
		return qListener;
		
		
	}
}
