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

	/**
	 * 信使
	 */
	private final CommLayer messenger;
	private final Member me;

	/**
	 * 发起的提案
	 */
	private final Map<Long, Proposal> proposals = new HashMap<>();

	/**
	 * 成功提交的信息
	 */
	private final Map<Long, Serializable> successfulMessages = new HashMap<>();

	/**
	 * 成功提交的信息id
	 */
	private final Map<Long, Long> successfulMsgIds = new HashMap<>();

	/**
	 * msgIds of messages that were not completed.
	 */
	private final HashSet<Long> messagesCirculating = new HashSet<>();
	@SuppressWarnings("rawtypes")
	private final List<MultiRequest> assistants = new LinkedList<>();

	private long viewNumber = 0;

	/**
	 * 提案的序列, 自增
	 */
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
			assistants.add(new Election(membership, messenger, time, newViewNumber()));
		}
	}

	/**
	 * 调度处理信息
	 * 并将交由assistants处理
	 * 根据多态，assistant可以识别不同的信息类型
	 * 进行对应的处理操作
	 *
	 * @param message 信息 远程的信息或本地发起的信息
	 */
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
				default:
					break;
			}
		}
		else if (message instanceof Tick)
		{
			update((Tick) message);
		}

		for (MultiRequest assistant : new ArrayList<>(assistants))
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

	/**
	 * 发送已经成功同步，但是accepter那边未拥有的信息给accepter
	 *
	 * @param missingSuccess 错过的日志的集合，accepter发送过来的
	 * @param sender accepter ip:port
	 */
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

	/**
	 * 生成新的任期
	 *
	 * update 2021-05-14：
	 * 使用递增的方式生成新任期
	 * 可以避免已经存在leader之后，又有新的leader篡位的情况出现
	 * 但是可能在集群一启动或者集群leader重新选举的时候进行多次选举的情况
	 */
	private long newViewNumber()
	{
//		int groupSize = membership.groupSize();
//		long previousBallot = viewNumber / groupSize;
//		viewNumber = (previousBallot + 1) * groupSize + membership.getPositionInGroup();
//		return viewNumber;
		return ++viewNumber;
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

	/**
	 * 注册节点的接收情况
	 *
	 * @param viewAccepted 某节点的注册情况
	 */
	private void registerViewAcceptance(ViewAccepted viewAccepted)
	{

		for (Long seqNo : viewAccepted.accepted.keySet())
		{
			Acceptance acceptance = viewAccepted.accepted.get(seqNo);
			Proposal proposal = proposals.get(seqNo);
			// 本地存储的该提案丢失，补全本地提案
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

	/**
	 * 注册节点的日志接收情况
	 *
	 * @param viewNo viewNumber
	 * @param seqNo 日志序号
	 * @param msgId 信息号
	 */
	private void registerAcceptance(long viewNo, long seqNo, long msgId)
	{
		proposals.get(seqNo).acceptDefault(viewNo, msgId);
	}

	private void abortBallot(long seqNo)
	{
		proposals.remove(seqNo);
	}

	/**
	 * 负责选举工作的助理
	 * 并且在选举过程中，同步各个节点之间的提案数据
	 */
	private class Election extends MultiRequest<NewView, ViewAccepted>
	{
		private final long viewNumber;

		public Election(GroupMembership membership, CommLayer messenger, long time, long viewNumber)
		{
			super(membership, messenger, new NewView(me, viewNumber), time);
			this.viewNumber = viewNumber;
		}

		/**
		 * 过滤remote-server的心跳信息
		 * 并且用于提案数据的同步工作
		 */
		@Override
		protected ViewAccepted filterResponse(Serializable message)
		{
			if (message instanceof ViewAccepted)
			{
				ViewAccepted viewAccepted = (ViewAccepted) message;
				if (viewAccepted.viewNumber != viewNumber) {
					return null;
				}
				// proposal sync
				registerViewAcceptance(viewAccepted);
				return viewAccepted;
			}
			else
			{
				return null;
			}
		}

		/**
		 * 当选leader
		 * 上任后，向paxos集群同步自己的信息
		 */
		@Override
		protected void onQuorumReached()
		{
			System.out.println(me + ": I am the leader");
			iAmElected = true;

			for (Long seqNo : proposals.keySet())
			{
				Proposal localProposal = proposals.get(seqNo);
				if (localProposal != null)
				{
					Serializable choice = localProposal.newestOutcome;
					long msgId = localProposal.getMsgId();
					messagesCirculating.add(msgId);
					// paxos集群同步proposal
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

	/**
	 * 负责 发送与接收 数据情况的助理
	 * 判断有几个节点收到该提案了（uncommitted）
	 */
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
				// 如果已经更换了任期了，或者当前处理的信息非此信息，则不处理
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

	/**
	 * 用于判断某一提案是否提交成功的助理
	 * 属于二阶段提交的第二阶段
	 */
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

		/**
		 * 一条提案成功被提交到各个节点
		 */
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
