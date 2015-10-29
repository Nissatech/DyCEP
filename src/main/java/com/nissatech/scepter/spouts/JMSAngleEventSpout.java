package com.nissatech.scepter.spouts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.nissatech.fitman.fitmaneventtypes.AngleSimpleEvent;
import com.nissatech.scepter.jms.IDestinationFactory;
import com.nissatech.scepter.jms.unmarshallers.JMSUnmarshaller;
import com.nissatech.scepter.jms.unmarshallers.UnmarshallingException;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.slf4j.LoggerFactory;

/**
 * Implements an JMS listening spout listening to destinations which messages contain HeartrateSimpleEvents. The events are unmarshalled before sent onto the "heartrateEvent" stream.
 * @author aleksandar
 */
public class JMSAngleEventSpout extends AbstractJMSSpout
{

    private static final long serialVersionUID = -3148088989845990968L;
    public static final String ANGLE_EVENT_STREAM= "angleEventStream";
    public static final String STARDOG_STREAM = "stardogStream";

   
    final private LinkedBlockingQueue<Message> messgeQueue;

    public JMSAngleEventSpout(final String id, final JMSUnmarshaller unmarshaller, ConnectionFactory connFactory, final IDestinationFactory destFactory, final String destination)
    {
        super(id, connFactory, unmarshaller, destFactory, destination);
        this.unmarshaller = unmarshaller;
        this.messgeQueue = new LinkedBlockingQueue<Message>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declareStream(ANGLE_EVENT_STREAM, new Fields("angleEvent"));
        declarer.declareStream(STARDOG_STREAM, new Fields("model"));
    }

    @Override
    public void nextTuple()
    {
        if (messgeQueue.isEmpty()) {
            Utils.sleep(50);
        }
        else {
            try {
                Message msg = messgeQueue.poll();

                //HeartrateSimpleEvent event = (HeartrateSimpleEvent) unmarshaller.unmarshal(msg);
                Object event = unmarshaller.unmarshal(msg);
                getOutputCollector().emit(ANGLE_EVENT_STREAM, new Values(event));

                /*Model m = createStorageModel(event);
                 getOutputCollector().emit(STARDOG_STREAM, new Values(m));*/
                
            }
            catch (UnmarshallingException ex) {
                LoggerFactory.getLogger(this.getClass().toString()).error(ex.toString());
            }
            catch (JMSException ex)
            {
                LoggerFactory.getLogger(this.getClass().toString()).error(ex.toString());
            }
        }

    }

    private Model createStorageModel(AngleSimpleEvent event)
    {
        ValueFactory factory = ValueFactoryImpl.getInstance();
        URI patient = factory.createURI("rdf:", "sensor" + event.getSensorId());
        URI hasHeartrate = factory.createURI("rdf:hasAngle");
        BNode hrBnode = factory.createBNode();
        URI hasValue = factory.createURI("rdf:hasValue");
        URI observationTime = factory.createURI("rdf:observationTime");
        Literal time = factory.createLiteral(new Date(event.getTimestamp()));
        Literal value = factory.createLiteral(event.getAngle());

        Model m = new LinkedHashModel();
        m.add(factory.createStatement(patient, hasHeartrate, hrBnode));
        m.add(factory.createStatement(hrBnode, hasValue, value));
        m.add(factory.createStatement(hrBnode, observationTime, time));
        return m;

    }

    @Override
    public void onMessage(final Message msg)
    {
        LoggerFactory.getLogger(this.getClass().toString()).info("Message received");
        this.messgeQueue.offer(msg);

    }

}
