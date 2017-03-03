package io.moquette.spi.impl;

import static io.moquette.spi.impl.ProtocolProcessor__.asStoredMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.parser.proto.messages.PublishMessage;
import io.moquette.server.netty.NettyUtils;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.impl.subscriptions.SubscriptionsStore;
import io.moquette.spi.security.IAuthorizator;
import io.netty.channel.Channel;

class Qos0PublishHandler__ implements PublishHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Qos0PublishHandler__.class);

    private final IAuthorizator m_authorizator;
    private final SubscriptionsStore subscriptions;
    private final IMessagesStore m_messagesStore;
    private final BrokerInterceptor__ m_interceptor;
    private final MessagesPublisher publisher;

    public Qos0PublishHandler__(IAuthorizator authorizator, SubscriptionsStore subscriptions,
                              IMessagesStore messagesStore, BrokerInterceptor__ interceptor,
                              MessagesPublisher messagesPublisher) {
        this.m_authorizator = authorizator;
        this.subscriptions = subscriptions;
        this.m_messagesStore = messagesStore;
        this.m_interceptor = interceptor;
        this.publisher = messagesPublisher;
    }

    /* (non-Javadoc)
	 * @see org.reactivetechnologies.io.moquette.spi.impl.PublishHandler#receive(io.netty.channel.Channel, io.moquette.parser.proto.messages.PublishMessage)
	 */
    @Override
	public void receive(Channel channel, PublishMessage msg) {
        //verify if topic can be write
        final String topic = msg.getTopicName();
        if (checkWriteOnTopic(topic, channel)) {
            return;
        }

        //route message to subscribers
        IMessagesStore.StoredMessage toStoreMsg = asStoredMessage(msg);
        String clientID = NettyUtils.clientID(channel);
        toStoreMsg.setClientID(clientID);

        LOG.debug("publish2Subscribers republishing to existing subscribers that matches the topic {}", topic);
        if (LOG.isTraceEnabled()) {
            LOG.trace("content <{}>", DebugUtils.payload2Str(toStoreMsg.getMessage()));
            LOG.trace("subscription tree {}", subscriptions.dumpTree());
        }
        
        //MODLOG: commented
        //List<Subscription> topicMatchingSubscriptions = subscriptions.matches(topic);
        //this.publisher.publish2Subscribers(toStoreMsg, topicMatchingSubscriptions);

        /*if (msg.isRetainFlag()) {
            //QoS == 0 && retain => clean old retained
            m_messagesStore.cleanRetained(topic);
        }*/

        String username = NettyUtils.userName(channel);
        m_interceptor.notifyTopicPublished(msg, clientID, username);
    }

    boolean checkWriteOnTopic(String topic, Channel channel) {
        String clientID = NettyUtils.clientID(channel);
        String username = NettyUtils.userName(channel);
        if (!m_authorizator.canWrite(topic, username, clientID)) {
            LOG.debug("topic {} doesn't have write credentials", topic);
            return true;
        }
        return false;
    }
}
