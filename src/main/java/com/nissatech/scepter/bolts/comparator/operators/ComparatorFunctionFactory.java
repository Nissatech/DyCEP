package com.nissatech.scepter.bolts.comparator.operators;


import com.nissatech.scepter.bolts.comparator.DatabaseComparatorBolt;
import com.nissatech.scepter.bolts.comparator.executor.BindingTupleComparator;
import java.io.Serializable;

/**
 *
 * @author aleksandar
 */
public class ComparatorFunctionFactory implements Serializable
{
    DatabaseComparatorBolt bolt;

    public ComparatorFunctionFactory()
    {
    }
    public BindingTupleComparator compareDbWithTuple(String dbBinding, String tupleBinding, CompareOperation operation)
    {
        return new BindingTupleComparator(dbBinding, tupleBinding, operation);
    }
    /*public ComparatorObject compareDbWithLiteral(String dbBinding, Object literal, CompareOperation operation)
    {
        
    }
    public ComparatorObject compareTupleWithLiteral(String tupleBinding, Object literal, CompareOperation operation)
    {
        
    }*/
    
}
