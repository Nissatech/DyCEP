package com.nissatech.scepter.jms;

import javax.jms.Destination;
import javax.jms.JMSException;

import javax.jms.Session;

/**
 *
 * @author aleksandar
 */
public class TopicFactory implements IDestinationFactory
{
    private static final long serialVersionUID = -5848844186152238878L;
    
    public TopicFactory() throws JMSException
    {
        
    }
    public Destination createDestination(Session session, String uri) throws JMSException
    {
        return session.createTopic(uri);
    }

}
