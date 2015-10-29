package com.nissatech.scepter.spouts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.nissatech.scepter.StreamDeclarations;
import com.nissatech.scepter.jms.IDestinationFactory;
import com.nissatech.scepter.jms.unmarshallers.JMSUnmarshaller;
import com.nissatech.scepter.jms.unmarshallers.UnmarshallingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.LoggerFactory;

/**
 *
 * @author aleksandar
 */
public class JMSReconfigurationSpout extends AbstractJMSSpout
{
    private static final long serialVersionUID = 960987154155052477L;

    final private LinkedBlockingQueue<Message> messgeQueue;

    public JMSReconfigurationSpout(String id, ConnectionFactory connFactory, JMSUnmarshaller unmarshaller, IDestinationFactory destFactory, List<String> destinationsIds)
    {
        super(id, connFactory, unmarshaller, destFactory, destinationsIds);
        this.messgeQueue = new LinkedBlockingQueue<Message>();
    }

    public JMSReconfigurationSpout(String id, ConnectionFactory connFactory, JMSUnmarshaller unmarshaller, IDestinationFactory destFactory, String destinationId)
    {
        super(id, connFactory, unmarshaller, destFactory, destinationId);
        this.messgeQueue = new LinkedBlockingQueue<Message>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declareStream(StreamDeclarations.SPOUT_PATTERN_CONFIGURATION_STREAM, new Fields("configuration"));
        declarer.declareStream(StreamDeclarations.SPOUT_EVENT_CONFIGURATION_STREAM, new Fields("configuration"));
    }

    @Override
    public void nextTuple()
    {
        if (messgeQueue.isEmpty())
        {
            Utils.sleep(50);
        }
        else
        {
            try
            {
                Message msg = messgeQueue.poll();
                Map event = (Map) unmarshaller.unmarshal(msg);
                
                String command = event.get("command").toString();
                if(command.equalsIgnoreCase("deploy") || command.equalsIgnoreCase("undeploy"))
                    getOutputCollector().emit(StreamDeclarations.SPOUT_PATTERN_CONFIGURATION_STREAM, new Values(event));
                else if(command.equalsIgnoreCase("register") || command.equalsIgnoreCase("unregister"))
                    getOutputCollector().emit(StreamDeclarations.SPOUT_EVENT_CONFIGURATION_STREAM, new Values(event));

            }
            catch (UnmarshallingException | JMSException ex)
            {
                LoggerFactory.getLogger(this.getClass().toString()).error(ex.toString());
            }
        }
    }

    @Override
    public void onMessage(Message msg)
    {
        LoggerFactory.getLogger(this.getClass().toString()).info("Configuration received");
        this.messgeQueue.offer(msg);
    }

}
