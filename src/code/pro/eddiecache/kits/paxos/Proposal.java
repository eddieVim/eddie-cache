package pro.eddiecache.kits.paxos;

import java.io.Serializable;

/**
 * Paxos提议
 * @author eddie
 */
public class Proposal
{
	/**
	 * 提议信息
	 */
	Serializable proposedMessage;
	long newestView;
	Serializable newestOutcome;
	long newestMsgId;
	Serializable choice;

	Proposal(long viewNo, Serializable proposedMessage, long msgId)
	{
		this.proposedMessage = proposedMessage;
		this.newestView = viewNo;
		this.newestOutcome = proposedMessage;
		this.newestMsgId = msgId;
	}

	public Serializable getChoice()
	{
		return choice;
	}

	/**
	 * 确定接收原来的提议
	 *
	 * @param viewNo viewNumber
	 * @param msgId 信息ID
	 */
	public void acceptDefault(long viewNo, long msgId)
	{
		acceptOutcome(viewNo, proposedMessage, msgId);
	}

	/**
	 * 接收该提议
	 * 若提案冲突（信息不一致），则以viewNo大的为准
	 */
	public void acceptOutcome(long viewNo, Serializable outcome, long msgId)
	{
		if (viewNo > newestView)
		{
			newestView = viewNo;
			newestOutcome = outcome;
			newestMsgId = msgId;
		}
	}

	public long getMsgId()
	{
		return newestMsgId;
	}
}
