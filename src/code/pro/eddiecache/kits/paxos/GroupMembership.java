package pro.eddiecache.kits.paxos;

import java.util.Collections;
import java.util.List;

import pro.eddiecache.kits.paxos.comm.Member;

/**
 * @author eddie
 */
public class GroupMembership
{
	/**
	 * PaxosGroup中的成员
	 */
	private final List<Member> members;

	/**
	 * 本机的信息
	 */
	private final Member me;

	/**
	 * 本机在group（members）中的位置
	 */
	private final int positionInGroup;

	public GroupMembership(List<Member> members, int i)
	{
		this(members, members.get(i));
	}

	public GroupMembership(List<Member> members, Member me)
	{
		this.members = members;
		Collections.sort(members);
		this.me = me;
		this.positionInGroup = findPositionInGroup(me, members);
	}

	/**
	 * 返回本机的ID
	 * @return 本机ID
	 */
	public Member getUID()
	{
		return me;
	}

	public int groupSize()
	{
		return members.size();
	}

	public List<Member> getMembers()
	{
		return members;
	}

	public int getPositionInGroup()
	{
		return positionInGroup;
	}

	public static int findPositionInGroup(Member me, List<Member> sortedMembers)
	{
		for (int i = 0; i < sortedMembers.size(); i++)
		{
			if (sortedMembers.get(i).equals(me)) {
				return i;
			}
		}
		throw new RuntimeException("Could not find " + me + " in " + sortedMembers.toString());
	}
}
