package com.nissatech.scepter.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 *
 * @author aleksandar
 */
public class QueueFactory implements IDestinationFactory
{
    private static final long serialVersionUID = -78749353210929837L;
    public QueueFactory() throws JMSException
    {
        super();
    }

    public Destination createDestination(Session session, String uri) throws JMSException
    {
        return session.createQueue(uri);
    }

}
