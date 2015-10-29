package com.nissatech.scepter;

/**
 *
 * Interface that should be inherited by all custom implemented Spouts and Bolts. Interface defines identification functionalities.
 * @author aleksandar
 */
public interface StormObject
{
    /**
     *  A function that returns an id of the Storm component. 
     *  The implementation may differ based on the element itself. This should be implemented for better management of the components while building the topology.
     * @return Returns an ID of the Storm component.  
     */
    String getId();
}
