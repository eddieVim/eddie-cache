package pro.eddiecache.kits.paxos.messages;

import java.io.Serializable;

/**
 * @author eddie
 */
public interface SpecialMessage extends Serializable
{
	MessageType getMessageType();

	enum MessageType
	{
		// 各种消息类型
		BROADCAST_REQ,
		BROADCAST,
		ACCEPT,
		ACCEPTED,
		JOIN,
		NEW_VIEW,
		SUCCESS,

		ABORT,
		PREVIOUS_OUTCOME,
		SUCCESS_ACK,
		VIEW_ACCEPTED
	}
}
