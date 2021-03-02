package pro.eddiecache.kits.paxos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import pro.eddiecache.kits.paxos.comm.CommLayer;
import pro.eddiecache.kits.paxos.comm.Member;
import pro.eddiecache.kits.paxos.comm.Tick;
import pro.eddiecache.kits.paxos.messages.Abort;
import pro.eddiecache.kits.paxos.messages.Accept;
import pro.eddiecache.kits.paxos.messages.Accepted;
import pro.eddiecache.kits.paxos.messages.BroadcastRequest;
import pro.eddiecache.kits.paxos.messages.NewView;
import pro.eddiecache.kits.paxos.messages.NoOp;
import pro.eddiecache.kits.paxos.messages.SpecialMessage;
import pro.eddiecache.kits.paxos.messages.Success;
import pro.eddiecache.kits.paxos.messages.SuccessAck;
import pro.eddiecache.kits.paxos.messages.ViewAccepted;

/**
 * @author eddie
 */
public class LeaderRole implements FailureListener
{
	private static final NoOp NO_OP = new NoOp();
	private final GroupMembership membership;
	private final CommLayer messenger;
	private final Member me;
	private final Map<Long, Proposal> proposals = new HashMap<Long, Proposal>();
	private final Map<Long, Serializable> successfulMessages = new HashMap<Long, Serializable>();
	private final Map<Long, Long> successfulMsgIds = new HashMap<Long, Long>();
	private final HashSet<Long> messagesCirculating = new HashSet<Long>(); // msgIds of messages that were not
	@SuppressWarnings("rawtypes")
	private final List<MultiRequest> assistants = new LinkedList<MultiRequest>();

	private long viewNumber = 0;
	private long seqNo = 0;
	private boolean iAmElected = false;
	private long time;

	public LeaderRole(GroupMembership membership, CommLayer commLayer, long time)
	{
		this.membership = membership;
		this.messenger = commLayer;
		this.time = time;
		this.me = membership.getUID();
		Member leader = PaxosUtils.selectLeader(membership.getMembers());
		if (leader.equals(me))
		{
			assistants.add(new Election(membership, messenger, time, viewNumber + newViewNumber()));
		}
	}

	@SuppressWarnings("rawtypes")
	public synchronized void dispatch(Serializable message)
	{
		if (message instanceof SpecialMessage)
		{
			SpecialMessage specialMessage = (SpecialMessage) message;
			switch (specialMessage.getMessageType())
			{
				case ABORT:
					onAbort((Abort) specialMessage);
					break;
				case BROADCAST_REQ:
					onBroadcastRequest((BroadcastRequest) specialMessage);
					break;
				case NEW_VIEW:
					onNewView((NewView) specialMessage);
					break;
			}
		}
		else if (message instanceof Tick)
		{
			update((Tick) message);
		}

		for (MultiRequest assistant : new ArrayList<MultiRequest>(assistants))
		{
			assistant.receive(message);
			if (assistant.isFinished())
			{
				assistants.remove(assistant);
			}
		}
	}

	/**
	 * 发起心跳
	 *
	 * @param tick 心跳
	 */
	@SuppressWarnings("rawtypes")
	public synchronized void update(Tick tick)
	{
		this.time = tick.time;
		for (MultiRequest assistant : assistants)
		{
			assistant.tick(tick.time);
		}
	}

	private void onNewView(NewView msg)
	{
		if (msg.viewNumber > this.viewNumber)
		{
			this.viewNumber = msg.viewNumber;
			if (!msg.leader.equals(me))
			{
				this.iAmElected = false;
			}
		}
	}

	private void onAbort(Abort abort)
	{
		abortBallot(abort.seqNo);
	}

	private void sendMissingSuccessMessages(Set<Long> missingSuccess, Member sender)
	{
		for (Long seqNo : missingSuccess)
		{
			if (successfulMessages.containsKey(seqNo))
			{
				Success message = new Success(seqNo, successfulMessages.get(seqNo), successfulMsgIds.get(seqNo));
				messenger.sendTo(sender, PaxosUtils.serialize(message));
			}
		}
	}

	private void onBroadcastRequest(BroadcastRequest req)
	{
		if (iAmElected)
		{
			if (messagesCirculating.contains(req.msgId)) {
				return;
			}
			messagesCirculating.add(req.msgId);
			createProposal(++seqNo, req.message, req.msgId);
			assistants.add(new MultiAccept(membership, messenger, seqNo, req.message, req.msgId));
		}
		else
		{
			System.out.println("I am not the leader");
		}
	}

	private long newViewNumber()
	{
		int groupSize = membership.groupSize();
		long previousBallot = viewNumber / groupSize;
		viewNumber = (previousBallot + 1) * groupSize + membership.getPositionInGroup();
		return viewNumber;
	}

	@Override
	public void memberFailed(Member failedMember, Set<Member> aliveMembers)
	{
		if (me.equals(PaxosUtils.selectLeader(aliveMembers)))
		{
			System.out.println(me + ": taking leadership");
			assistants.add(new Election(membership, messenger, time, newViewNumber()));
		}
	}

	private void registerViewAcceptance(ViewAccepted viewAccepted)
	{

		for (Long seqNo : viewAccepted.accepted.keySet())
		{
			Acceptance acceptance = viewAccepted.accepted.get(seqNo);
			Proposal proposal = proposals.get(seqNo);
			if (proposal == null)
			{
				proposals.put(seqNo, new Proposal(acceptance.viewNumber, acceptance.message, acceptance.msgId));
			}
			else
			{
				proposal.acceptOutcome(acceptance.viewNumber, acceptance.message, acceptance.msgId);
			}
		}
	}

	private void createProposal(long seqNo, Serializable message, long msgId)
	{
		proposals.put(seqNo, new Proposal(viewNumber, message, msgId));
	}

	private void registerAcceptance(long viewNo, long seqNo, long msgId)
	{
		proposals.get(seqNo).acceptDefault(viewNo, msgId);
	}

	private void abortBallot(long seqNo)
	{
		proposals.remove(seqNo);
	}

	private class Election extends MultiRequest<NewView, ViewAccepted>
	{
		private final long viewNumber;

		public Election(GroupMembership membership, CommLayer messenger, long time, long viewNumber)
		{
			super(membership, messenger, new NewView(me, viewNumber), time);
			this.viewNumber = viewNumber;
		}

		@Override
		protected ViewAccepted filterResponse(Serializable message)
		{
			if (message instanceof ViewAccepted)
			{
				ViewAccepted viewAccepted = (ViewAccepted) message;
				if (viewAccepted.viewNumber != viewNumber) {
					return null;
				}
				registerViewAcceptance(viewAccepted);
				return viewAccepted;
			}
			else
			{
				return null;
			}
		}

		@Override
		protected void onQuorumReached()
		{
			System.out.println(me + ": I am the leader");
			iAmElected = true;

			for (Long seqNo : proposals.keySet())
			{
				Proposal proposal = proposals.get(seqNo);
				if (proposal != null)
				{
					Serializable choice = proposal.newestOutcome;
					long msgId = proposal.getMsgId();
					messagesCirculating.add(msgId);
					assistants.add(new MultiAccept(membership, messenger, seqNo, choice, msgId));
				}
			}

			LeaderRole.this.seqNo = PaxosUtils.findMax(proposals.keySet());
			for (long seqNo = 1; seqNo < LeaderRole.this.seqNo; seqNo++)
			{
				if (!proposals.containsKey(seqNo))
				{
					createProposal(seqNo, NO_OP, 0L);
					assistants.add(new MultiAccept(membership, messenger, seqNo, NO_OP, 0L));
				}
			}
		}
	}

	private class MultiAccept extends MultiRequest<Accept, Accepted>
	{
		private final long seqNo;
		private final Serializable message;
		private final long msgId;

		public MultiAccept(GroupMembership membership, CommLayer messenger, long seqNo, Serializable message,
				long msgId)
		{
			super(membership, messenger, new Accept(viewNumber, seqNo, message, msgId, me), time);
			this.seqNo = seqNo;
			this.message = message;
			this.msgId = msgId;
		}

		@Override
		protected Accepted filterResponse(Serializable message)
		{
			if (message instanceof Accepted)
			{
				Accepted accepted = (Accepted) message;
				if (accepted.viewNo != viewNumber || accepted.seqNo != seqNo)
				{
					return null;
				}
				registerAcceptance(accepted.viewNo, accepted.seqNo, accepted.msgId);
				sendMissingSuccessMessages(accepted.missingSuccess, accepted.sender);
				return accepted;
			}
			else
			{
				return null;
			}
		}

		@Override
		protected void onQuorumReached()
		{
			successfulMessages.put(seqNo, message);
			successfulMsgIds.put(seqNo, msgId);
			assistants.add(new MultiSuccess(membership, messenger, seqNo, message, msgId));
		}
	}

	private class MultiSuccess extends MultiRequest<Success, SuccessAck>
	{
		private final long seqNo;
		private final long msgId;

		public MultiSuccess(GroupMembership membership, CommLayer messenger, long seqNo, Serializable msg, long msgId)
		{
			super(membership, messenger, new Success(seqNo, msg, msgId), time);
			this.seqNo = seqNo;
			this.msgId = msgId;
		}

		@Override
		protected SuccessAck filterResponse(Serializable message)
		{
			if (message instanceof SuccessAck)
			{
				SuccessAck successAck = (SuccessAck) message;
				return (msgId != successAck.getMsgId()) ? null : successAck;
			}
			else
			{
				return null;
			}
		}

		@Override
		protected void onCompleted()
		{

			successfulMessages.remove(seqNo);
			successfulMsgIds.remove(msgId);
			messagesCirculating.remove(msgId);
			finish();
		}
	}
}
