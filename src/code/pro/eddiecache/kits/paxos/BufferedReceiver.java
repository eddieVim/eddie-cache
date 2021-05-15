package pro.eddiecache.kits.paxos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import pro.eddiecache.kits.paxos.messages.NoOp;

/**
 * @author eddie
 */
public class BufferedReceiver
{
	private final Receiver receiver;
	/**
	 * 本节点上已经接受的MSG
	 * k-v: seqNo - MSG
	 */
	private final Map<Long, Serializable> receiverBuffer = new HashMap<>();

	private long receivedNo = -1;

	public BufferedReceiver(Receiver receiver)
	{
		this.receiver = receiver;
	}

	/**
	 * 接受来自leader的信息
	 * 1 非NoOp，则进行同步信息
	 * 2 NoOp信息，用于线性补全信息，即将每个当前节点之前的每一条log都被chosen
	 */
	public void receive(long seqNo, Serializable message)
	{
		if (receiver != null)
		{
			receiverBuffer.put(seqNo, message);
			while (receiverBuffer.containsKey(receivedNo + 1))
			{
				receivedNo++;
				Serializable messageToDeliver = receiverBuffer.get(receivedNo);
				if (!(messageToDeliver instanceof NoOp))
				{
					receiver.receive(messageToDeliver);
				}
				receiverBuffer.remove(receivedNo);
			}
		}
	}
}
