package pro.eddiecache.kits.paxos.messages;

import java.io.Serializable;

/**
 * @author eddie
 * 一朝天子一朝臣，新leader上任，要求各地补交前朝的亏空
 * 此log被写入集群中，即可保证前面View任期的Log被提交
 */
public class NoOp implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Override
	public boolean equals(Object obj)
	{
		return obj instanceof NoOp;
	}
}
