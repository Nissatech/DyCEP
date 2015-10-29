package com.nissatech.scepter;

/**
 *
 * @author aleksandar
 */
public class StreamDeclarations
{
    //OUTGRESS FROM SPOUTS
    public static String SPOUT_EVENT_STREAM = "spoutEsperEvents";
    public static String SPOUT_PATTERN_CONFIGURATION_STREAM = "spoutEsperPatternConfiguration";
    public static String SPOUT_EVENT_CONFIGURATION_STREAM = "spoutEsperEventConfiguration";
    
    //public static String ESPER_EVENT_STREAM = "esperIncomingEvents";
    
    //OUTGRESS FROM THE CONFIGURATION BOLT
    public static String ESPER_PATTERN_CONFIGURATION_STREAM = "esperPatternIncomingConfiguration";
    public static String ESPER_EVENT_CONFIGURATION_STREAM = "esperEventIncomingConfiguration";
    
    //TO THE RPC - RESPONDER
    public static String RPC_RESPONSE_STREAM = "rpcResponseStream";
}
