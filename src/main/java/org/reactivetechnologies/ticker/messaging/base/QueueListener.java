package org.reactivetechnologies.ticker.messaging.base;

import org.reactivetechnologies.ticker.messaging.Data;

/**
 * Callback on a CMQ. Do NOT use this interface directly. Subclass from {@linkplain AbstractQueueListener}.
 * @author esutdal
 * @see AbstractQueueListener
 * @see QueueContainer#register(QueueListener)
 * @param <T>
 */
public interface QueueListener<T extends Data> extends Consumer<T> {

	/**
	 * The type of {@linkplain Data} this listener is receiving.
	 * @return
	 */
	Class<T> dataType();
	/**
	 * Unique identifier for this queue listener
	 * @return
	 */
	String identifier();
	/**
	 * Max parallelism to achieve
	 * @return
	 */
	int parallelism();
	
	/**
	 * The routing key (queue name) for the given exchange.
	 * @return
	 */
	String routing();

}