package pro.eddiecache.kits.paxos;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import pro.eddiecache.kits.paxos.comm.CommLayer;
import pro.eddiecache.kits.paxos.comm.Member;
import pro.eddiecache.kits.paxos.comm.Tick;
import pro.eddiecache.kits.paxos.messages.Heartbeat;

/**
 * @author eddie
 * 负责监视paxos集群中的存活情况，收发心跳
 */
public class FailureDetector
{
	private static final long INTERVAL = 1000;
	private static final long TIMEOUT = 3000;

	private final GroupMembership membership;
	private final CommLayer messenger;
	private final FailureListener listener;
	private final Map<Member, Long> lastHeardFrom = new ConcurrentHashMap<Member, Long>();
	private final byte[] heartbeat;

	private final Set<Member> membersAlive = new HashSet<>();
	private long lastHearbeat = 0;
	private long time = 0;

	public FailureDetector(final GroupMembership membership, final CommLayer messenger, final FailureListener listener)
	{
		this.membership = membership;
		this.messenger = messenger;
		this.listener = listener;

		membersAlive.addAll(membership.getMembers());

		heartbeat = PaxosUtils.serialize(new Heartbeat(membership.getUID()));
	}

	private void sendHeartbeat(long time)
	{
		messenger.sendTo(membership.getMembers(), heartbeat);
		this.lastHearbeat = time;
	}

	/**
	 * 检查是否存在失效的节点，timeout时间内未通信
	 *
	 * @param time 时间
	 */
	private void checkForFailedMembers(long time)
	{
		for (Member member : membership.getMembers())
		{
			if (member.equals(membership.getUID()))
			{
				continue;
			}

			if (!lastHeardFrom.containsKey(member))
			{
				initialize(time, member);
			}

			if (time - lastHeardFrom.get(member) > TIMEOUT)
			{
				if (membersAlive.contains(member))
				{
					membersAlive.remove(member);
					listener.memberFailed(member, membersAlive);
				}
			}
			else
			{
				membersAlive.add(member);
			}
		}
	}

	private void initialize(long time, Member member)
	{
		lastHeardFrom.put(member, time);
	}

	public void update(long time)
	{
		this.time = time;
		if (lastHearbeat + INTERVAL < time)
		{
			sendHeartbeat(time);
		}
		checkForFailedMembers(time);
	}

	/**
	 * FailureDetector use to monitor paxos-cluster.
	 *
	 * @param message remote server or local server message, heartbeat is remote, tick is local.
	 */
	public void dispatch(Serializable message)
	{
		if (message instanceof Heartbeat)
		{
			Heartbeat heartbeat = (Heartbeat) message;
			lastHeardFrom.put(heartbeat.sender, time);
		}
		else if (message instanceof Tick)
		{
			update(((Tick) message).time);
		}
	}
}
