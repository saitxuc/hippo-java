package com.hippo.bootstrap.formatter;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

import javax.management.AttributeList;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

/**
 * 
 * @author saitxuc
 * 2015-3-19
 */
public interface OutputFormatter {
	
	/**
     * Retrieve the output stream being used by the formatter
     */
    OutputStream getOutputStream();

    /**
     * Print an ObjectInstance format of an mbean
     * @param mbean - mbean to print
     */
    void printMBean(ObjectInstance mbean);

    /**
     * Print an ObjectName format of an mbean
     * @param mbean - mbean to print
     */
    void printMBean(ObjectName mbean);

    /**
     * Print an AttributeList format of an mbean
     * @param mbean - mbean to print
     */
    void printMBean(AttributeList mbean);

    /**
     * Print a Map format of an mbean
     * @param mbean - mbean to print
     */
    @SuppressWarnings("rawtypes")
    void printMBean(Map mbean);

    /**
     * Print a Collection format of mbeans
     * @param mbean - collection of mbeans
     */
    @SuppressWarnings("rawtypes")
    void printMBean(Collection mbean);

    /**
     * Print a Map format of a JMS message
     * @param msg
     */
    @SuppressWarnings("rawtypes")
    void printMessage(Map msg);

    /**
     * Print help messages
     * @param helpMsgs - help messages to print
     */
    void printHelp(String[] helpMsgs);

    /**
     * Print an information message
     * @param info - information message to print
     */
    void printInfo(String info);

    /**
     * Print an exception message
     * @param e - exception to print
     */
    void printException(Exception e);

    /**
     * Print a version information
     * @param version - version info to print
     */
    void printVersion(String version);

    /**
     * Print a generic key value mapping
     * @param map to print
     */
    @SuppressWarnings("rawtypes")
    void print(Map map);

    /**
     * Print a generic array of strings
     * @param strings - string array to print
     */
    void print(String[] strings);

    /**
     * Print a collection of objects
     * @param collection - collection to print
     */
    @SuppressWarnings("rawtypes")
    void print(Collection collection);

    /**
     * Print a java string
     * @param string - string to print
     */
    void print(String string);
	
}
