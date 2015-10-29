package com.nissatech.scepter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.nissatech.fitman.fitmaneventtypes.AngleSimpleEvent;
import com.nissatech.scepter.bolts.LoggerBolt;
import com.nissatech.scepter.bolts.StardogBolt;
import com.nissatech.scepter.bolts.comparator.DatabaseComparatorBolt;
import com.nissatech.scepter.bolts.comparator.operators.ComparatorFunctionFactory;
import com.nissatech.scepter.bolts.comparator.operators.GreaterThanOperation;
import com.nissatech.scepter.bolts.esperbolt.EsperBolt;
import com.nissatech.scepter.jms.IDestinationFactory;
import com.nissatech.scepter.jms.TopicFactory;
import com.nissatech.scepter.jms.unmarshallers.EventJMSUnmarshaller;
import com.nissatech.scepter.spouts.AbstractJMSSpout;
import com.nissatech.scepter.spouts.JMSAngleEventSpout;
import java.util.Scanner;
import javax.jms.JMSException;
import javax.xml.datatype.DatatypeConfigurationException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.joda.time.DateTime;

/**
 *
 * @author aleksandar 
 * The class containing the main function which creates and
 * configures spouts, bolts and deploys the topology.
 */
public class Executor
{

    public static final boolean PRODUCTION = false; //do not forget to exclude the storm dependancies also

    /**
     *
     * Function configures spouts and bolts and deploys the topology.
     *
     * @throws JMSException
     * @throws InterruptedException
     * @throws DatatypeConfigurationException
     */
    public static final void main(String args[]) throws JMSException, InterruptedException, DatatypeConfigurationException, AlreadyAliveException, InvalidTopologyException
    {
        //initializeSimpleTopology();
        initializeTopology();

    }

    /**
     * Blocking function awaiting the termination command. The command is "end"
     * on the stdin.
     */
    private static void waitEndCmd()
    {
        Scanner keyboardScanner = new Scanner(System.in);
        while (!keyboardScanner.nextLine().equalsIgnoreCase("end"))
        {
        };
    }

    private static void initializeTopology() throws JMSException
    {
        //building topology

        //spouts
        //<editor-fold desc="Spouts">
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
        IDestinationFactory destFactory = new TopicFactory();

        EventJMSUnmarshaller unmarshaller = new EventJMSUnmarshaller();
        AbstractJMSSpout spout = new JMSAngleEventSpout("angleSpout", unmarshaller, connectionFactory, destFactory, "nissatech.scepter.events");
         //</editor-fold>

        //bolts
        //<editor-fold desc="Bolts">
        EsperBolt esperBolt = new EsperBolt.Builder("esper-bolt").inputs()
                .aliasStream(spout.getId(), JMSAngleEventSpout.ANGLE_EVENT_STREAM).withFields("angle").ofType(AngleSimpleEvent.class).toEventType("AngleEvent")
                .outputs().onDefaultStream().emit("avgAn", "cnt")
                .statements()
                .add("select avg(AngleEvent.angleEvent.angle) as avgAn, count(*) as cnt from AngleEvent.std:groupwin(AngleEvent.angleEvent.sensorId).win:time_batch(30 sec) group by AngleEvent.angleEvent.sensorId").build();

        
        LoggerBolt printerBolt = new LoggerBolt("printerBolt", "avgAn", "cnt", "result");
        StardogBolt stardogBolt = new StardogBolt("stardogBolt");

        ComparatorFunctionFactory cff = new ComparatorFunctionFactory();

        DatabaseComparatorBolt comparator = new DatabaseComparatorBolt.Builder("main-comparator").tupleQuery("select (avg(?angle) as ?avgAn) where { <rdf:sensor_1> <rdf:hasAngle> ?hrObj . ?hrObj <rdf:hasValue> ?angle ; <rdf:observationTime> ?time . FILTER(?time < ?timeLimit) }")
                .bindLiteral("timeLimit", new DateTime())
                .comparisons().comparison(cff.compareDbWithTuple("avgAn", "avgAn", new GreaterThanOperation()).and(cff.compareDbWithTuple("avgAn", "avgAn", new GreaterThanOperation())))
                .emitToDefaultStream()
                .emitLiteral("result", "YAHOO!").build();

         //</editor-fold>
        //topology initialization in LocalCluster
        //<editor-fold desc="Topology initialization">
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spout.getId(), spout);
        
        builder.setBolt(esperBolt.getId(), esperBolt)
                .shuffleGrouping(spout.getId(), JMSAngleEventSpout.ANGLE_EVENT_STREAM);

        //builder.setBolt(printerBolt.getId(), printerBolt).shuffleGrouping(esperBolt.getId()).shuffleGrouping(comparator.getId());
        builder.setBolt(stardogBolt.getId(), stardogBolt).shuffleGrouping(spout.getId(), JMSAngleEventSpout.STARDOG_STREAM);
        //builder.setBolt(comparator.getId(), comparator).shuffleGrouping(esperBolt.getId());
        

        Config conf = new Config();
        conf.put("E4FD_host", "localhost");
        conf.put("E4FD_port", 8080);
        conf.put("Output_host", "localhost");
        conf.put("Output_port", "8084");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("scepter", conf, builder.createTopology());
        //</editor-fold>

        waitEndCmd();
        cluster.shutdown();

    }
}
