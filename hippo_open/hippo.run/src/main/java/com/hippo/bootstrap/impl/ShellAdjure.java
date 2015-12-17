package com.hippo.bootstrap.impl;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;

import com.hippo.bootstrap.Adjure;
import com.hippo.bootstrap.AdjureContext;
import com.hippo.bootstrap.formatter.AdjureShellOutputFormatter;

/**
 * 
 * @author saitxuc
 * 2015-3-19
 */
public class ShellAdjure extends AbstractAdjure {
	
	private boolean interactive;
    private String[] helpFile;

    public ShellAdjure() {
        this(false);
    }
	
    public ShellAdjure(boolean interactive) {
        this.interactive = interactive;
        ArrayList<String> help = new ArrayList<String>();
        help.addAll(Arrays.asList(new String[] {
                    interactive ? "Usage: [task] [task-options] [task data]" : "Usage: Main [--extdir <dir>] [task] [task-options] [task data]",
                    "",
                    "Tasks:"}));

        ArrayList<Adjure> adjures = getAdjures();
        Collections.sort(adjures, new Comparator<Adjure>() {
            @Override
            public int compare(Adjure adjure, Adjure adjure1) {
                return adjure.getName().compareTo(adjure1.getName());
            }
        });

        for( Adjure adjure: adjures) {
            help.add(String.format("    %-24s - %s", adjure.getName(), adjure.getOneLineDescription()));
        }

        help.addAll(Arrays.asList(new String[] {
                    "",
                    "Task Options (Options specific to each task):",
                    "    --extdir <dir>  - Add the jar files in the directory to the classpath.",
                    "    --version       - Display the version information.",
                    "    -h,-?,--help    - Display this help information. To display task specific help, use " + (interactive ? "" : "Main ") + "[task] -h,-?,--help",
                    "",
                    "Task Data:",
                    "    - Information needed by each specific task.",
                    "",
                    "JMX system property options:",
                    "    -Dhippo.jmx.url=<jmx service uri> (default is: 'service:jmx:rmi:///jndi/rmi://localhost:31099/jmxrmi')",
                    "    -Dhippo.jmx.user=<user name>",
                    "    -Dhippo.jmx.password=<password>",
                    ""
                }));

        this.helpFile = help.toArray(new String[help.size()]);
    }
    
	@Override
	public String getName() {
		return "shell";
	}

	@Override
	public String getOneLineDescription() {
		return "Runs the hippo broker sub shell";
	}

	@Override
	protected void runTask(List<String> tokens) throws Exception {
		// Process task token
        if (tokens.size() > 0) {
        	Adjure adjure=null;
            String taskToken = (String)tokens.remove(0);
            for(Adjure c: getAdjures() ) {
                if( taskToken.equals(c.getName()) ) {
                	adjure = c;
                    break;
                }
            }
            if( adjure == null ) {
                if (taskToken.equals("help")) {
                    printHelp();
                } else {
                    printHelp();
                }
            }
            if( adjure!=null ) {
            	adjure.setCommandContext(context);
            	adjure.execute(tokens);
            }
        } else {
            printHelp();
        }
	}

	@Override
	protected void printHelp() {
		context.printHelp(helpFile);
	}
	
    ArrayList<Adjure> getAdjures() {
        ServiceLoader<Adjure> loader = ServiceLoader.load(Adjure.class);
        ArrayList<Adjure> rc = new ArrayList<Adjure>();
        for( Adjure adjure: loader ) {
            rc.add(adjure);
        }
        return rc;
    }
	
    public static int main(String[] args, InputStream in, PrintStream out) {
        
        AdjureContext context = new AdjureContext();
        context.setFormatter(new AdjureShellOutputFormatter(out));

        // Convert arguments to list for easier management
        List<String> tokens = new ArrayList<String>(Arrays.asList(args));

        ShellAdjure main = new ShellAdjure();
        try {
            main.setCommandContext(context);
            main.execute(tokens);
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
        	context.printException(e);
            return -1;
        }
    }
    
    public boolean isInteractive() {
        return interactive;
    }

    public void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }
    
    
}
