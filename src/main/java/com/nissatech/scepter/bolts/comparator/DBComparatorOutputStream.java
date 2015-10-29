package com.nissatech.scepter.bolts.comparator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author aleksandar
 */
public class DBComparatorOutputStream implements Serializable
{
    private final String streamName;
    private final List<DBComparatorOutputLiteral> literals;
    private final List<String> forwardingTuples;
    

    public DBComparatorOutputStream(String name, List<DBComparatorOutputLiteral> list ,List<String> tuples)
    {
        this.streamName = name;
        this.literals = list;
        this.forwardingTuples= tuples;
    }

    public List<DBComparatorOutputLiteral> getTuples()
    {
        return literals;
    }

    public String getStreamName()
    {
        return streamName;
    }
    public List<String> getTupleNames()
    {
        ArrayList<String> tupleNames = new ArrayList();
        for(DBComparatorOutputLiteral outputLiteral : this.literals)
            tupleNames.add(outputLiteral.getName());
        tupleNames.addAll(forwardingTuples);
        return tupleNames;
    }
     public List<Object> getLiteralValues()
    {
        ArrayList<Object> tupleValues = new ArrayList();
        for(DBComparatorOutputLiteral outputLiteral : this.literals)
            tupleValues.add(outputLiteral.getValue());
        return tupleValues;
    }

    public List<String> getForwardingTuples()
    {
        return forwardingTuples;
    }
     
   
    
    
    
}
