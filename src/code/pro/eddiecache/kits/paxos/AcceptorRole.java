package pro.eddiecache.kits.paxos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pro.eddiecache.kits.paxos.comm.CommLayer;
import pro.eddiecache.kits.paxos.comm.Member;
import pro.eddiecache.kits.paxos.messages.Abort;
import pro.eddiecache.kits.paxos.messages.Accept;
import pro.eddiecache.kits.paxos.messages.Accepted;
import pro.eddiecache.kits.paxos.messages.BroadcastRequest;
import pro.eddiecache.kits.paxos.messages.NewView;
import pro.eddiecache.kits.paxos.messages.SpecialMessage;
import pro.eddiecache.kits.paxos.messages.Success;
import pro.eddiecache.kits.paxos.messages.SuccessAck;
import pro.eddiecache.kits.paxos.messages.ViewAccepted;

/**
 * @author eddie
 */
public class AcceptorRole
{
	private static final Log log = LogFactory.getLog(AcceptorRole.class);

	public static final long MAX_CIRCULATING_MESSAGES = 1000000L;
	private final GroupMembership membership;
	private final CommLayer messenger;
	private final BufferedReceiver receiver;
	private final Member me;
	private final WaitingTimer waitingForResponse = new WaitingTimer();
	private final int myPositionInGroup;

	/**
	 * what we accepted for each seqNo
	 */
	Map<Long, Acceptance> accepted = new HashMap<>();

	private Member leader;
	private long viewNumber;

	/**
	 * missing SUCCESS messages
	 */
	private MissingMessagesTracker missing = new MissingMessagesTracker();
	private AtomicLong msgIdGenerator = new AtomicLong(0);

	public AcceptorRole(GroupMembership membership, CommLayer messenger, Receiver receiver)
	{
		this.membership = membership;
		this.messenger = messenger;
		this.receiver = new BufferedReceiver(receiver);
		this.me = membership.getUID();
		this.myPositionInGroup = membership.getPositionInGroup();
		this.leader = me;
	}

	/**
	 * 广播信息
	 * 先发送给leader
	 * 再由leader发送给集群中的各个节点
	 * 并监听是否广播成功
	 *
	 * @param message 信息
	 */
	public void broadcast(Serializable message)
	{
		long msgId = createMsgId(message);
		boolean broadcastSuccessful = false;
		try
		{
			while (!broadcastSuccessful)
			{
				messenger.sendTo(leader, PaxosUtils.serialize(new BroadcastRequest(message, msgId)));
				broadcastSuccessful = waitingForResponse.waitALittle(msgId);
			}
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 生成一个消息Id
	 */
	private long createMsgId(Serializable message)
	{
		return myPositionInGroup * MAX_CIRCULATING_MESSAGES
				+ msgIdGenerator.incrementAndGet() % MAX_CIRCULATING_MESSAGES;
	}

	public void dispatch(Serializable message)
	{
		if (message instanceof SpecialMessage)
		{
			SpecialMessage specialMessage = (SpecialMessage) message;
			switch (specialMessage.getMessageType())
			{
				case NEW_VIEW:
					onNewView((NewView) specialMessage);
					break;
				case ACCEPT:
					onAccept((Accept) specialMessage);
					break;
				case SUCCESS:
					onSuccess((Success) specialMessage);
					break;
				default:
					break;
			}
		}
	}

	/**
	 * 进入一个新的view (Leader选举)
	 */
	private void onNewView(NewView newView)
	{
		if (newView.viewNumber >= viewNumber)
		{
			if (log.isDebugEnabled()) {
				log.debug(me + ": vote to " + newView.leader);
			}
			this.leader = newView.leader;
			this.viewNumber = newView.viewNumber;
			messenger.sendTo(leader, PaxosUtils.serialize(new ViewAccepted(viewNumber, accepted, me)));
		}
	}

	/**
	 * 接收信息
	 *
	 * @param accept 相关信息
	 */
	private void onAccept(Accept accept)
	{
		// 是上一个任期的数据，不进行处理
		if (accept.viewNo < viewNumber)
		{
			messenger.sendTo(accept.sender, PaxosUtils.serialize(new Abort(accept.viewNo, accept.seqNo)));
		}
		else
		{
			accepted.put(accept.seqNo, new Acceptance(accept.viewNo, accept.message, accept.msgId));
			messenger.sendTo(accept.sender, PaxosUtils.serialize(
					new Accepted(accept.viewNo, accept.seqNo, accept.msgId, missing.getMissing(accept.seqNo), me)));
		}
	}

	/**
	 * 二阶段提交的commit
	 *
	 * @param success Paxos-cluster半数以上同步成功，commit
	 */
	private void onSuccess(Success success)
	{
		receiver.receive(success.seqNo, success.message);
		missing.received(success.seqNo);
		waitingForResponse.unblock(success.msgId);
		messenger.sendTo(leader, PaxosUtils.serialize(new SuccessAck(success.msgId, me)));
	}
}
