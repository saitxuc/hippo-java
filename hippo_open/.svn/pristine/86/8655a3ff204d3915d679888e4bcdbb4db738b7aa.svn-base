package com.pinganfu.hippo.bootstrap.impl;

import java.util.List;

import com.pinganfu.hippo.bootstrap.Adjure;
import com.pinganfu.hippo.bootstrap.AdjureContext;

/**
 * 
 * @author saitxuc
 * 2014-3-19
 */
public abstract class AbstractAdjure implements Adjure{
	public static final String COMMAND_OPTION_DELIMETER = ",";
	public static final String PROVIDER_VERSION = "v1.0.0";
    private boolean isPrintHelp;
    private boolean isPrintVersion;

    protected AdjureContext context;

    public void setCommandContext(AdjureContext context) {
        this.context = context;
    }
    
    /**
     * 
     */
    public void execute(List<String> tokens) throws Exception {
        // Parse the options specified by "-"
        parseOptions(tokens);

        // Print the help file of the task
        if (isPrintHelp) {
            printHelp();

            // Print the hippo version
        } else if (isPrintVersion) {
            context.printVersion(PROVIDER_VERSION);

            // Run the specified task
        } else {
            runTask(tokens);
        }
    }

    /**
     * 
     * @param tokens
     * @throws Exception
     */
    protected void parseOptions(List<String> tokens) throws Exception {
        while (!tokens.isEmpty()) {
            String token = tokens.remove(0);
            if (token.startsWith("-")) {
                // Token is an option
                handleOption(token, tokens);
            } else {
                // Push back to list of tokens
                tokens.add(0, token);
                return;
            }
        }
    }

    /**
     * 
     * @param token
     * @param tokens
     * @throws Exception
     */
    protected void handleOption(String token, List<String> tokens) throws Exception {
        isPrintHelp = false;
        isPrintVersion = false;
        // If token is a help option
        if (token.equals("-h") || token.equals("-?") || token.equals("--help")) {
            isPrintHelp = true;
            tokens.clear();

            // If token is a version option
        } else if (token.equals("--version")) {
            isPrintVersion = true;
            tokens.clear();
        } else if (token.startsWith("-D")) {
            // If token is a system property define option
            String key = token.substring(2);
            String value = "";
            int pos = key.indexOf("=");
            if (pos >= 0) {
                value = key.substring(pos + 1);
                key = key.substring(0, pos);
            }
            System.setProperty(key, value);
        } else {
            // Token is unrecognized
            context.printInfo("Unrecognized option: " + token);
            isPrintHelp = true;
        }
    }

    /**
     * Run the specific task.
     * 
     * @param tokens - command arguments
     * @throws Exception
     */
    protected abstract void runTask(List<String> tokens) throws Exception;

    /**
     * Print the help messages for the specific task
     */
    protected abstract void printHelp();
}
