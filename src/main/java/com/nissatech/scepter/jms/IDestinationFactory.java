package com.nissatech.scepter.jms;

import java.io.Serializable;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 *
 * @author aleksandar
 */
public interface IDestinationFactory extends Serializable
{
    Destination createDestination(Session session, String uri) throws JMSException;
}
