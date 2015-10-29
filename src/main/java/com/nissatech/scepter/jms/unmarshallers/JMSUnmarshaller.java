package com.nissatech.scepter.jms.unmarshallers;

import java.io.Serializable;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 *
 * @author aleksandar
 */
public interface JMSUnmarshaller extends Serializable
{
    Object unmarshal(Message message) throws UnmarshallingException, JMSException;
}
