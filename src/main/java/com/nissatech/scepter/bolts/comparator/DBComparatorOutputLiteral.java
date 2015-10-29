package com.nissatech.scepter.bolts.comparator;

import java.io.Serializable;

/**
 *
 * @author aleksandar
 */
public class DBComparatorOutputLiteral implements Serializable
{
    protected String name;
    protected Object value;
    
    public DBComparatorOutputLiteral(String name, Object value)
    {
       this.name=name;
       this.value=value;
    }
     public String getName()
    {
        return name;
    }

    public Object getValue()
    {
        return value;
    }
    
    
}
