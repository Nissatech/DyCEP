package com.nissatech.scepter.jms.unmarshallers;

import com.espertech.esper.adapter.InputAdapter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;
import org.slf4j.LoggerFactory;
import org.springframework.jms.support.converter.SimpleMessageConverter;

/**
 *
 * @author aleksandar
 */
public class EventJMSUnmarshaller implements JMSUnmarshaller
{

    private static final long serialVersionUID = 2199361181654145063L;

    @Override
    public Object unmarshal(Message message) throws UnmarshallingException
    {
        try
        {
            if (message instanceof ObjectMessage)
            {
                ObjectMessage objmsg = (ObjectMessage) message;
                try
                {
                    return objmsg.getObject();
                }
                catch (JMSException ex)
                {
                    throw new UnmarshallingException(ex.toString(), ex);
                }

            }
            if (message instanceof TextMessage)
            {
                TextMessage objmsg = (TextMessage) message;
                try
                {
                    return objmsg.getText();
                }
                catch (JMSException ex)
                {
                    throw new UnmarshallingException(ex.toString(), ex);
                }

            }

            else if (message instanceof MapMessage)
            {
                Map<String, Object> properties = new HashMap<>();
                MapMessage mapMsg = (MapMessage) message;
                Enumeration en = mapMsg.getMapNames();
                while (en.hasMoreElements())
                {

                    String property = (String) en.nextElement();
                    Object mapObject = mapMsg.getObject(property);
                    properties.put(property, mapObject);
                }

                Object typeProperty = properties.get(InputAdapter.ESPERIO_MAP_EVENT_TYPE);
                if (typeProperty == null)
                {
                    //LoggerFactory.getLogger(this.getClass().toString()).warn(".unmarshal Failed to unmarshal map message, expected type property not found: '" + InputAdapter.ESPERIO_MAP_EVENT_TYPE + "'");
                    throw new UnmarshallingException("unmarshal Failed to unmarshal map message, expected type property not found: '" + InputAdapter.ESPERIO_MAP_EVENT_TYPE + "'");
                }
                return properties;

            }
            else if (message instanceof BytesMessage)
            {
                SimpleMessageConverter converter = new SimpleMessageConverter();
                byte[] bytes = (byte[]) converter.fromMessage(message);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream oos = null;

                try
                {
                    oos = new ObjectInputStream(bais);
                    Object deserializedObject = oos.readObject();
                    if (!(deserializedObject instanceof Map))
                    {
                        throw new MessageTypeNotSupportedException(message);
                    }
                    if (!((Map) deserializedObject).containsKey(InputAdapter.ESPERIO_MAP_EVENT_TYPE))
                    {
                        throw new UnmarshallingException("unmarshal Failed to unmarshal map message, expected type property not found: '" + InputAdapter.ESPERIO_MAP_EVENT_TYPE + "'");
                    }
                    return deserializedObject;
                }
                catch (ClassNotFoundException ex)
                {
                    LoggerFactory.getLogger(this.getClass().toString()).error(ex.toString());
                    throw new UnmarshallingException(ex.toString(), ex);

                }
                catch (IOException ex)
                {
                    LoggerFactory.getLogger(this.getClass().toString()).error(ex.toString());
                }
                finally
                {
                    if (oos != null)
                    {
                        try
                        {
                            oos.close();
                        }
                        catch (IOException ex)
                        {
                            LoggerFactory.getLogger(this.getClass().toString()).error(ex.toString());
                        }
                    }
                }

            }
            else
            {
                throw new MessageTypeNotSupportedException(message);
            }
        }
        catch (JMSException ex)
        {
            LoggerFactory.getLogger(this.getClass().toString()).error(ex.toString());
        }
        return null;

    }

}
