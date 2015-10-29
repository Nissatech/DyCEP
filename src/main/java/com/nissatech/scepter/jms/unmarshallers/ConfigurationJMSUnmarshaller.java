/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nissatech.scepter.jms.unmarshallers;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;

/**
 *
 * @author aleksandar
 */
public class ConfigurationJMSUnmarshaller implements JMSUnmarshaller
{

    public Object unmarshal(Message message) throws UnmarshallingException, JMSException
    {
        if (message instanceof MapMessage)
        {

            Map<String, Object> properties = new HashMap<String, Object>();
            MapMessage mapMsg = (MapMessage) message;
            Enumeration en = mapMsg.getMapNames();
            while (en.hasMoreElements())
            {

                String property = (String) en.nextElement();
                Object mapObject = mapMsg.getObject(property);
                properties.put(property, mapObject);
            }
            return properties;

        }
        else
        {
            throw new MessageTypeNotSupportedException(message);
        }
    }

}
