package com.nissatech.scepter.bolts.comparator.operators;

import org.openrdf.model.Value;

/**
 *
 * @author aleksandar
 */
public class GreaterThanOperation implements CompareOperation
{
    private static final long serialVersionUID = -2906916732555530767L;

    public boolean execute(Value x, Object y)
    {
        Double NumX = Double.valueOf(x.stringValue());
        Double NumY = (Double)y;
        return NumX > NumY;
    }

}
