package pro.eddiecache.kits.paxos;

import java.util.HashSet;
import java.util.Set;

/**
 * @author eddie
 * use this class to track messages.
 * All data up to that tail index is stored and accepted.
 *
 */
public class MissingMessagesTracker
{
	private long tail = 0;

	/**
	 * have been received and store in.
	 */
	private final Set<Long> received = new HashSet<>();

	public void received(long seqNo)
	{
		if (tail == seqNo)
		{
			tail++;
			advanceTail();
		}
		else
		{
			received.add(seqNo);
		}
	}

	private void advanceTail()
	{
		while (!received.isEmpty())
		{
			if (!received.contains(tail))
			{
				return;
			}
			received.remove(tail);
			tail++;
		}
	}

	/**
	 * 每收到一个新的消息，
	 * 都试图去查询[tail, seqNo)这个区间内缺失的msg，
	 * 并发送给leader进行补全
	 *
	 * @param seqNo 新的消息的seqNo
	 */
	public Set<Long> getMissing(long seqNo)
	{
		Set<Long> missingSuccess = new HashSet<>();
		for (long i = tail; i < seqNo; i++)
		{
			if (!received.contains(i))
			{
				missingSuccess.add(i);
			}
		}
		return missingSuccess;
	}
}
