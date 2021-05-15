package pro.eddiecache.kits.paxos.messages;

import java.util.Set;

import pro.eddiecache.kits.paxos.comm.Member;

/**
 * @author eddie
 * Accepted有成员发出，告知leader，消息已经接受
 */
public class Accepted implements SpecialMessage, MessageWithSender
{
	private static final long serialVersionUID = 1L;
	public long viewNo;
	public long seqNo;
	public long msgId;
	public Set<Long> missingSuccess;
	public Member sender;

	public Accepted(long viewNo, long seqNo, long msgId, Set<Long> missingSuccess, Member me)
	{
		this.viewNo = viewNo;
		this.seqNo = seqNo;
		this.msgId = msgId;
		this.missingSuccess = missingSuccess;
		sender = me;
	}

	@Override
	public MessageType getMessageType()
	{
		return MessageType.ACCEPTED;
	}

	@Override
	public String toString()
	{
		return "ACCEPTED " + msgId + " missing(" + missingSuccess + ") from " + sender;
	}

	@Override
	public Member getSender()
	{
		return sender;
	}
}
