package com.hippo.bootstrap;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Comparator; 


/**
 * 
 * @author saitxuc 2015-3-19
 */
public class Main {
	public static final String TASK_DEFAULT_CLASS = "com.hippo.bootstrap.impl.ShellAdjure";
	private static boolean useDefExt = true;
	
	private File hippoHome;
    private File hippoBase;
    private ClassLoader classLoader;
    private final Set<File> extensions = new LinkedHashSet<File>();
    private final Set<File> hippoClassPath = new LinkedHashSet<File>();
    
    
    public static void main(String[] args) {
    	Main app = new Main();

        // Convert arguments to collection for easier management
        List<String> tokens = new LinkedList<String>(Arrays.asList(args));
        // Parse for extension directory option
        app.parseExtensions(tokens);
        File confDir = app.getHippoConfig();
        app.addClassPath(confDir);
        
     // Add the following to the classpath:
        //
        // ${hippo.base}/conf
        // ${hippo.base}/lib/* (only if hippo.base != hippo.home)
        // ${hippo.home}/lib/*
        // ${hippo.base}/lib/optional/* (only if hippo.base !=
        // hippo.home)
        // ${hippo.home}/lib/optional/*
        //
        
        app.addClassPathList(System.getProperty("hippo.classpath"));
        
        try {
            app.runTaskClass(tokens);
            System.exit(0);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        	System.out.println("Could not load class: " + e.getMessage());
            try {
                ClassLoader cl = app.getClassLoader();
                if (cl != null) {
                    System.out.println("Class loader setup: ");
                    printClassLoaderTree(cl);
                }
            } catch (MalformedURLException e1) {
            }
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
        	System.out.println("Failed to execute main task. Reason: " + e);
            System.exit(1);
        }
    }
	
    public void addExtensionDirectory(File directory) {
        extensions.add(directory);
    }
    
    public boolean canUseExtdir() {
        try {
            Main.class.getClassLoader().loadClass(TASK_DEFAULT_CLASS);
            return false;
        } catch (ClassNotFoundException e) {
            return true;
        }
    }
    
    private static int printClassLoaderTree(ClassLoader cl) {
        int depth = 0;
        if (cl.getParent() != null) {
            depth = printClassLoaderTree(cl.getParent()) + 1;
        }

        StringBuffer indent = new StringBuffer();
        for (int i = 0; i < depth; i++) {
            indent.append("  ");
        }

        if (cl instanceof URLClassLoader) {
            URLClassLoader ucl = (URLClassLoader)cl;
            System.out.println(indent + cl.getClass().getName() + " {");
            URL[] urls = ucl.getURLs();
            for (int i = 0; i < urls.length; i++) {
                System.out.println(indent + "  " + urls[i]);
            }
            System.out.println(indent + "}");
        } else {
            System.out.println(indent + cl.getClass().getName());
        }
        return depth;
    }
    
    public void parseExtensions(List<String> tokens) {
        if (tokens.isEmpty()) {
            return;
        }

        int count = tokens.size();
        int i = 0;

        // Parse for all --extdir and --noDefExt options
        while (i < count) {
            String token = tokens.get(i);
            // If token is an extension dir option
            if (token.equals("--extdir")) {
                // Process token
                count--;
                tokens.remove(i);

                // If no extension directory is specified, or next token is
                // another option
                if (i >= count || tokens.get(i).startsWith("-")) {
                    System.out.println("Extension directory not specified.");
                    System.out.println("Ignoring extension directory option.");
                    continue;
                }

                // Process extension dir token
                count--;
                File extDir = new File(tokens.remove(i));

                if (!canUseExtdir()) {
                    System.out.println("Extension directory feature not available due to the system classpath being able to load: " + TASK_DEFAULT_CLASS);
                    System.out.println("Ignoring extension directory option.");
                    continue;
                }

                if (!extDir.isDirectory()) {
                    System.out.println("Extension directory specified is not valid directory: " + extDir);
                    System.out.println("Ignoring extension directory option.");
                    continue;
                }

                addExtensionDirectory(extDir);
            } else if (token.equals("--noDefExt")) { // If token is
                // --noDefExt option
                count--;
                tokens.remove(i);
                useDefExt = false;
            } else {
                i++;
            }
        }
     }
    
    public File getHippoConfig() {
        File hippoConfig = null;

        if (System.getProperty("hippo.conf") != null) {
        	hippoConfig = new File(System.getProperty("hippo.conf"));
        } else {
        	hippoConfig = new File(getHippoBase() + "/conf");
        }
        return hippoConfig;
    }
    
    public File getHippoBase() {
        if (hippoBase == null) {
            if (System.getProperty("hippo.base") != null) {
            	hippoBase = new File(System.getProperty("hippo.base"));
            }

            if (hippoBase == null) {
            	hippoBase = getHippoHome();
                System.setProperty("hippo.base", hippoBase.getAbsolutePath());
            }
        }

        return hippoBase;
    }
    
    public File getHippoHome() {
        if (hippoHome == null) {
            if (System.getProperty("hippo.home") != null) {
            	hippoHome = new File(System.getProperty("hippo.home"));
            }
            if (hippoHome == null) {
                // guess from the location of the jar
                URL url = Main.class.getClassLoader().getResource("com/hippo/bootstrap/Main.class");
                if (url != null) {
                    try {
                        JarURLConnection jarConnection = (JarURLConnection)url.openConnection();
                        url = jarConnection.getJarFileURL();
                        URI baseURI = new URI(url.toString()).resolve("..");
                        hippoHome = new File(baseURI).getCanonicalFile();
                        System.setProperty("hippo.home", hippoHome.getAbsolutePath());
                    } catch (Exception ignored) {
                    }
                }
            }

            if (hippoHome == null) {
            	hippoHome = new File("../.");
                System.setProperty("hippo.home", hippoHome.getAbsolutePath());
            }
        }

        return hippoHome;
    }
    
    public void addClassPath(File classpath) {
        hippoClassPath.add(classpath);
    }
    
    public void addClassPathList(String fileList) {
        if (fileList != null && fileList.length() > 0) {
            StringTokenizer tokenizer = new StringTokenizer(fileList, ";");
            while (tokenizer.hasMoreTokens()) {
                addClassPath(new File(tokenizer.nextToken()));
            }
        }
    }
    
    public void runTaskClass(List<String> tokens) throws Throwable {

        StringBuilder buffer = new StringBuilder();
        buffer.append(System.getProperty("java.vendor"));
        buffer.append(" ");
        buffer.append(System.getProperty("java.version"));
        buffer.append(" ");
        buffer.append(System.getProperty("java.home"));
        System.out.println("Java Runtime: " + buffer.toString());

        buffer = new StringBuilder();
        buffer.append("current=");
        buffer.append(Runtime.getRuntime().totalMemory()/1024L);
        buffer.append("k  free=");
        buffer.append(Runtime.getRuntime().freeMemory()/1024L);
        buffer.append("k  max=");
        buffer.append(Runtime.getRuntime().maxMemory()/1024L);
        buffer.append("k");
        System.out.println("  Heap sizes: " + buffer.toString());

        List<?> jvmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        buffer = new StringBuilder();
        for (Object arg : jvmArgs) {
            buffer.append(" ").append(arg);
        }
        System.out.println("    JVM args:" + buffer.toString());
        System.out.println("Extensions classpath:\n  " + getExtensionDirForLogging());

        System.out.println("HIPPO_HOME: " + getHippoHome());
        System.out.println("HIPPO_BASE: " + getHippoBase());
        System.out.println("HIPPO_CONF: " + getHippoConfig());
        System.out.println("HIPPO_DATA: " + getHippoDataDir());

        ClassLoader cl = getClassLoader();
        Thread.currentThread().setContextClassLoader(cl);

        // Use reflection to run the task.
        try {
            String[] args = tokens.toArray(new String[tokens.size()]);
            Class<?> task = cl.loadClass(TASK_DEFAULT_CLASS);
            Method runTask = task.getMethod("main", new Class[] {
                String[].class, InputStream.class, PrintStream.class
            });
            runTask.invoke(task.newInstance(), args, System.in, System.out);
            
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        	throw e.getCause();
        }
    }
    
    public ClassLoader getClassLoader() throws MalformedURLException {
        if (classLoader == null) {
            // Setup the ClassLoader
            classLoader = Main.class.getClassLoader();
            if (!extensions.isEmpty() || !hippoClassPath.isEmpty()) {

                ArrayList<URL> urls = new ArrayList<URL>();

                for (Iterator<File> iter = hippoClassPath.iterator(); iter.hasNext();) {
                    File dir = iter.next();
                    urls.add(dir.toURI().toURL());
                }

                for (Iterator<File> iter = extensions.iterator(); iter.hasNext();) {
                    File dir = iter.next();
                    if (dir.isDirectory()) {
                        File[] files = dir.listFiles();
                        if (files != null) {

                            // Sort the jars so that classpath built is consistently in the same
                            // order. Also allows us to use jar names to control classpath order.
                            Arrays.sort(files, new Comparator<File>() {
                                public int compare(File f1, File f2) {
                                    return f1.getName().compareTo(f2.getName());
                                }
                            });

                            for (int j = 0; j < files.length; j++) {
                                if (files[j].getName().endsWith(".zip") || files[j].getName().endsWith(".jar")) {
                                    urls.add(files[j].toURI().toURL());
                                }
                            }
                        }
                    }
                }

                URL u[] = new URL[urls.size()];
                urls.toArray(u);
                classLoader = new URLClassLoader(u, classLoader);
            }
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        return classLoader;
    }
    
    
    public File getHippoDataDir() {
        File hippoDataDir = null;

        if (System.getProperty("hippo.data") != null) {
        	hippoDataDir = new File(System.getProperty("hippo.data"));
        } else {
        	hippoDataDir = new File(getHippoBase() + "/data");
        }
        return hippoDataDir;
    }
    public String getExtensionDirForLogging() {
        StringBuilder sb = new StringBuilder("[");
        for (Iterator<File> it = extensions.iterator(); it.hasNext();) {
            File file = it.next();
            sb.append(file.getPath());
            if (it.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }
    
}
