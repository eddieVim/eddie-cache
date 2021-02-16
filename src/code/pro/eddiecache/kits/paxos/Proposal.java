package pro.eddiecache.kits.paxos;

import java.io.Serializable;

public class Proposal
{
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

	public void acceptDefault(long viewNo, long msgId)
	{
		acceptOutcome(viewNo, proposedMessage, msgId);
	}

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
