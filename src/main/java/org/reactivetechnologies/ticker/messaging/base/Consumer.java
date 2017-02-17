package org.reactivetechnologies.ticker.messaging.base;

import org.reactivetechnologies.ticker.messaging.Data;

public interface Consumer<T extends Data>{

	/**
	 * Callback method invoked on message added to queue.
	 * 
	 * @param <T>
	 * @param m
	 * @throws Exception
	 */
	void onMessage(T m) throws Exception;
	/**
	 * Lifecycle method on shutdown. This will be invoked after the executor threads
	 * have shutdown. So it can be assumed that no {@link #onMessage()} will be executing
	 * when this is invoked.
	 */
	void destroy();
	/**
	 * Lifecycle method on startup. May throw an unchecked exception.
	 */
	void init();
}