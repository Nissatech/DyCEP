package com.nissatech.scepter.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.nissatech.scepter.StormObject;


import com.nissatech.scepter.jms.IDestinationFactory;
import com.nissatech.scepter.jms.unmarshallers.JMSUnmarshaller;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import org.apache.activemq.ActiveMQSession;

/**
 * Abstract spout which implements a JMS message listener. Designed to listen certain destinations for messages, deserialize/unmarshall them and forward them.
 * @author aleksandar
 */
public abstract class AbstractJMSSpout extends BaseRichSpout implements MessageListener, StormObject
{

    private static final long serialVersionUID = 3932944718091237070L;
    
    protected List<Destination> destinations;
    protected List<String> destinationIds;
    protected List<MessageConsumer> consumers;
    protected ConnectionFactory connFactory;
    protected IDestinationFactory destFactory;
    protected JMSUnmarshaller unmarshaller;
    private SpoutOutputCollector output;
    protected Session session;
    protected Connection connection;
    private final String id;

    public AbstractJMSSpout(String id, final ConnectionFactory connFactory, final JMSUnmarshaller unmarshaller,  final IDestinationFactory destFactory, final List<String> destinationsIds)
    {
        super();
        this.destinations = new ArrayList<Destination>();
        this.destinationIds = destinationsIds;
        this.consumers = new ArrayList<MessageConsumer>();
        this.connFactory = connFactory;
        this.destFactory = destFactory;
        this.id=id;
        this.unmarshaller=unmarshaller;

    }
    public AbstractJMSSpout(String id, final ConnectionFactory connFactory, final JMSUnmarshaller unmarshaller, final IDestinationFactory destFactory, final String destinationId)
    {
        super();
        this.destinations = new ArrayList<Destination>();
        this.destinationIds = new ArrayList<String>();
        this.consumers = new ArrayList<MessageConsumer>();
        this.connFactory = connFactory;
        this.destFactory = destFactory;
        this.id=id;
        this.destinationIds.add(destinationId);
        this.unmarshaller=unmarshaller;
    }

    @Override
    public void close()
    {
        try {
            for (MessageConsumer c : this.consumers) {
                c.close();
            }

            
            if(this.session!=null) this.session.close();
            if(this.connection != null) 
            {
                this.connection.stop();
                this.connection.close();
            }
            
            super.close();
        }
        catch (JMSException ex) {
            Logger.getLogger(AbstractJMSSpout.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void open(Map conf, TopologyContext tc, SpoutOutputCollector soc)
    {
        try {
            String username=null,password=null;
            if(conf.containsKey("jmsUser"))
                username=conf.get("jmsUser").toString();
            if(conf.containsKey("jmsPass"))
                password=conf.get("jmsPass").toString();
            
            this.output = soc;
            this.connection = connFactory.createConnection(username,password);
            connection.start();
            this.session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);   
            this.initDestinations();
        }
        catch (JMSException ex) {
            Logger.getLogger(AbstractJMSSpout.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void initDestinations()
    {
        try {
             for (String destination : this.destinationIds) {
                Destination dest = destFactory.createDestination(this.session, destination);
                MessageConsumer consumer = session.createConsumer(dest);
                consumer.setMessageListener(this);

                destinations.add(dest);
                consumers.add(consumer);
            }
        }
        catch (JMSException ex) {
            Logger.getLogger(AbstractJMSSpout.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    protected SpoutOutputCollector getOutputCollector()
    {
        return this.output;
    }
    
    @Override
    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);

    @Override
    public abstract void nextTuple();
    
    @Override
    public abstract void onMessage(Message msg);

    public String getId()
    {
        return this.id;
    }
}
