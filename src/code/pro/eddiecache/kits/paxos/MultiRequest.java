package pro.eddiecache.kits.paxos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import pro.eddiecache.kits.paxos.comm.CommLayer;
import pro.eddiecache.kits.paxos.comm.Member;
import pro.eddiecache.kits.paxos.comm.Tick;
import pro.eddiecache.kits.paxos.messages.MessageWithSender;

/**
 * @author eddie
 */
public abstract class MultiRequest<T extends Serializable, R extends MessageWithSender>
{
	public static final int RESEND_INTERVAL = 1000;
	private final GroupMembership membership;
	protected final CommLayer messenger;
	protected final byte[] request;
	protected Map<Member, R> responses = new HashMap<>();
	private long lastResend;
	private boolean finished = false;
	private boolean quorumHasBeenReached = false;
	private boolean allMembersHaveReplied = false;

	public MultiRequest(GroupMembership membership, CommLayer messenger, T req, long time)
	{
		this.membership = membership;
		this.messenger = messenger;
		this.request = PaxosUtils.serialize(req);
		messenger.sendTo(membership.getMembers(), this.request);
		this.lastResend = time;
	}

	@SuppressWarnings("unchecked")
	protected R filterResponse(Serializable message)
	{
		try
		{
			return (R) message;
		}
		catch (ClassCastException e)
		{
			return null;
		}
	}

	/**
	 * 达到半数以上
	 */
	protected void onQuorumReached()
	{
	}

	protected void onCompleted()
	{
		finish();
	}

	public void tick(long time)
	{
		if (time > lastResend + RESEND_INTERVAL)
		{
 			resendRequests(time);
		}
	}

	protected boolean haveQuorum()
	{
		return responses.size() > membership.groupSize() / 2;
	}

	final protected void finish()
	{
		this.finished = true;
	}

	public boolean isFinished()
	{
		return finished;
	}

	final public void receive(Serializable message)
	{
		if (message instanceof Tick)
		{
			tick(((Tick) message).time);
		}

		R resp = filterResponse(message);
		if (resp != null)
		{
			responses.put(resp.getSender(), resp);
			if (haveQuorum() && !quorumHasBeenReached)
			{
				onQuorumReached();
				quorumHasBeenReached = true;
			}
			if (allMembersReplied() && !allMembersHaveReplied)
			{
				onCompleted();
				allMembersHaveReplied = true;
			}
		}
	}

	protected void resendRequests(long time)
	{
		for (Member member : membership.getMembers())
		{
			if (!responses.containsKey(member))
			{
				messenger.sendTo(member, request);
			}
		}
		lastResend = time;
	}

	protected boolean allMembersReplied()
	{
		return responses.size() == membership.groupSize();
	}
}
