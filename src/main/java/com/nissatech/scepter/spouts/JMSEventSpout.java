package com.nissatech.scepter.spouts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.nissatech.scepter.jms.IDestinationFactory;
import com.nissatech.scepter.jms.unmarshallers.JMSUnmarshaller;
import com.nissatech.scepter.jms.unmarshallers.UnmarshallingException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.LoggerFactory;

/**
 *
 * @author aleksandar
 */
public class JMSEventSpout extends AbstractJMSSpout
{
    private static final long serialVersionUID = 1984992882826087368L;

    final private LinkedBlockingQueue<Message> messgeQueue;
    String outputStream;
    String outputField;
    public JMSEventSpout(String id, ConnectionFactory connFactory, JMSUnmarshaller unmarshaller, IDestinationFactory destFactory, List<String> destinationsIds, String outputStream, String outputField)
    {
        super(id, connFactory, unmarshaller, destFactory, destinationsIds);
        this.messgeQueue = new LinkedBlockingQueue<>();
        this.outputStream=outputStream;
        this.outputField = outputField;
    }

    public JMSEventSpout(String id, ConnectionFactory connFactory, JMSUnmarshaller unmarshaller, IDestinationFactory destFactory, String destinationId, String outputStream, String outputField)
    {
        super(id, connFactory, unmarshaller, destFactory, destinationId);
        this.messgeQueue = new LinkedBlockingQueue<>();
        this.outputStream=outputStream;        
        this.outputField = outputField;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declareStream(outputStream, new Fields(this.outputField));
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
                Object event = unmarshaller.unmarshal(msg);
                getOutputCollector().emit(outputStream, new Values(event));
                
            }
            catch (UnmarshallingException | JMSException ex) {
                LoggerFactory.getLogger(this.getClass().toString()).error(ex.toString());
            }
        }
    }

    @Override
    public void onMessage(Message msg)
    {
        LoggerFactory.getLogger(this.getClass().toString()).info("Message received");
        this.messgeQueue.offer(msg);
    }
    
}
