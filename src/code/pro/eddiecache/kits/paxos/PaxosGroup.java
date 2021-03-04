package pro.eddiecache.kits.paxos;

import java.io.Serializable;
import java.net.SocketException;
import java.net.UnknownHostException;

import pro.eddiecache.kits.paxos.comm.CommLayer;
import pro.eddiecache.kits.paxos.comm.UDPMessenger;

/**
 * @author eddie
 */
public class PaxosGroup implements CommLayer.MessageListener
{
	private final AcceptorRole acceptorRole;
	private final LeaderRole leaderRole;
	private final FailureDetector failureDetector;
	private final CommLayer commLayer;

	public PaxosGroup(GroupMembership membership, Receiver receiver) throws SocketException, UnknownHostException
	{
		// 创建信使
		this(membership, new UDPMessenger(membership.getUID().getPort()), receiver);
	}

	public PaxosGroup(GroupMembership membership, CommLayer commLayer, Receiver receiver)
	{
		this(membership, commLayer, receiver, System.currentTimeMillis());
	}

	public PaxosGroup(GroupMembership membership, CommLayer commLayer, Receiver receiver, long time)
	{
		this.commLayer = commLayer;

		leaderRole = new LeaderRole(membership, commLayer, time);
		acceptorRole = new AcceptorRole(membership, commLayer, receiver);
		failureDetector = new FailureDetector(membership, commLayer, leaderRole);

		this.commLayer.setListener(this);
	}

	/**
	 * 广播信息
	 *
	 * @param message 信息
	 */
	public void broadcast(Serializable message)
	{
		acceptorRole.broadcast(message);
	}

	public void close()
	{
		commLayer.close();
	}

	/**
	 * 调度信息
	 *
	 * @param message remote server receive or local tick send.
	 */
	private void dispatch(Serializable message)
	{
		leaderRole.dispatch(message);
		acceptorRole.dispatch(message);
		failureDetector.dispatch(message);
	}

	@Override
	public void receive(byte[] message)
	{
		dispatch((Serializable) PaxosUtils.deserialize(message));
	}
}
