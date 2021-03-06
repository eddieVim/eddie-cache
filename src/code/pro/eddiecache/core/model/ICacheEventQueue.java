package pro.eddiecache.core.model;

import java.io.IOException;

import pro.eddiecache.core.stats.IStats;

/**
 * @author eddie
 */
public interface ICacheEventQueue<K, V>
{
	enum QueueType
	{
		// 队列线程类型
		SINGLE,
		POOLED
	}

	QueueType getQueueType();

	void addPutEvent(ICacheElement<K, V> ce) throws IOException;

	void addRemoveEvent(K key) throws IOException;

	void addRemoveAllEvent() throws IOException;

	void addDisposeEvent() throws IOException;

	long getListenerId();

	void destroy();

	boolean isAlive();

	boolean isWorking();

	int size();

	boolean isEmpty();

	IStats getStatistics();
}
