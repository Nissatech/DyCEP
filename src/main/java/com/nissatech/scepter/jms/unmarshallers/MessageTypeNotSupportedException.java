/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.nissatech.scepter.jms.unmarshallers;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 *
 * @author aleksandar
 */
public class MessageTypeNotSupportedException extends UnmarshallingException
{

    public MessageTypeNotSupportedException(Message message)
    {
        super("The unmarshaller cannot unmarshal the " + message.getClass().getName() + " message type.");
    }    
}
