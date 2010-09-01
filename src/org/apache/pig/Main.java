/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.text.ParseException;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;
import jline.History;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.tools.timer.PerformanceTimerFactory;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;

/**
 * Main class for Pig engine.
 */
@InterfaceAudience.LimitedPrivate({"Oozie"})
@InterfaceStability.Stable
public class Main {
   
    private final static Log log = LogFactory.getLog(Main.class);
    
    private static final String LOG4J_CONF = "log4jconf";
    private static final String BRIEF = "brief";
    private static final String DEBUG = "debug";
    private static final String VERBOSE = "verbose";
    
    private enum ExecMode {STRING, FILE, SHELL, UNKNOWN}    
                
/**
 * The Main-Class for the Pig Jar that will provide a shell and setup a classpath appropriate
 * for executing Jar files.  Warning, this method calls System.exit().
 * 
 * @param args
 *            -jar can be used to add additional jar files (colon separated). - will start a
 *            shell. -e will execute the rest of the command line as if it was input to the
 *            shell.
 * @throws IOException
 */
public static void main(String args[]) {
    System.exit(run(args, null));
}

static int run(String args[], PigProgressNotificationListener listener) {
    int rc = 1;
    
    GenericOptionsParser parser = new GenericOptionsParser(args);
    Configuration conf = parser.getConfiguration();    
    
    Properties properties = new Properties();
    PropertiesUtil.loadDefaultProperties(properties);
    properties.putAll(ConfigurationUtil.toProperties(conf));
    
    String[] pigArgs = parser.getRemainingArgs();
    
    boolean verbose = false;
    boolean gruntCalled = false;
    String logFileName = null;
    boolean userSpecifiedLog = false;
    
    boolean checkScriptOnly = false;

    try {
        BufferedReader pin = null;
        boolean debug = false;
        boolean dryrun = false;
        ArrayList<String> params = new ArrayList<String>();
        ArrayList<String> paramFiles = new ArrayList<String>();
        HashSet<String> optimizerRules = new HashSet<String>();

        CmdLineParser opts = new CmdLineParser(pigArgs);
        opts.registerOpt('4', "log4jconf", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('b', "brief", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('c', "check", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('d', "debug", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('e', "execute", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('f', "file", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('h', "help", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('i', "version", CmdLineParser.ValueExpected.OPTIONAL);        
        opts.registerOpt('l', "logfile", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('m', "param_file", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('p', "param", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('r', "dryrun", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('t', "optimizer_off", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('v', "verbose", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('w', "warning", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('x', "exectype", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('F', "stop_on_failure", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('M', "no_multiquery", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('P', "propertyFile", CmdLineParser.ValueExpected.REQUIRED);

        ExecMode mode = ExecMode.UNKNOWN;
        String file = null;
        ExecType execType = ExecType.MAPREDUCE ;
        String execTypeString = properties.getProperty("exectype");
        if(execTypeString!=null && execTypeString.length()>0){
            execType = PigServer.parseExecType(execTypeString);
        }
                
        if (properties.getProperty("aggregate.warning") == null) {
            //by default warning aggregation is on
            properties.setProperty("aggregate.warning", ""+true);
        }
                
        if (properties.getProperty("opt.multiquery") == null) {
            //by default multiquery optimization is on
            properties.setProperty("opt.multiquery", ""+true);
        }

        if (properties.getProperty("stop.on.failure") == null) {
            //by default we keep going on error on the backend
            properties.setProperty("stop.on.failure", ""+false);
        }
        
        // set up client side system properties in UDF context
        UDFContext.getUDFContext().setClientSystemProps();

        char opt;
        while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
            switch (opt) {
            case '4':
                String log4jconf = opts.getValStr();
                if(log4jconf != null){
                    properties.setProperty(LOG4J_CONF, log4jconf);
                }
                break;

            case 'b':
                properties.setProperty(BRIEF, "true");
                break;

            case 'c': 
                checkScriptOnly = true;                
                break;

            case 'd':
                String logLevel = opts.getValStr();
                if (logLevel != null) {
                    properties.setProperty(DEBUG, logLevel);
                }
                debug = true;
                break;
                
            case 'e': 
                mode = ExecMode.STRING;
                break;

            case 'f':
                mode = ExecMode.FILE;
                file = opts.getValStr();
                break;

            case 'F':
                properties.setProperty("stop.on.failure", ""+true);
                break;

            case 'h':
		String topic = opts.getValStr();
		if (topic != null)
		    if (topic.equalsIgnoreCase("properties"))
		        printProperties();
                    else{
                        System.out.println("Invalide help topic - " + topic);
			usage();
                    }
		else
                    usage();
                return ReturnCode.SUCCESS;

            case 'i':
            	System.out.println(getVersionString());
            	return ReturnCode.SUCCESS;

            case 'l':
                //call to method that validates the path to the log file 
                //and sets up the file to store the client side log file                
                String logFileParameter = opts.getValStr();
                if (logFileParameter != null && logFileParameter.length() > 0) {
                    logFileName = validateLogFile(logFileParameter, null);
                } else {
                    logFileName = validateLogFile(logFileName, null);
                }
                userSpecifiedLog = true;
                properties.setProperty("pig.logfile", (logFileName == null? "": logFileName));
                break;

            case 'm':
                paramFiles.add(opts.getValStr());
                break;

            case 'M':
                // turns off multiquery optimization
                properties.setProperty("opt.multiquery",""+false);
                break;
                            
            case 'p': 
                params.add(opts.getValStr());
                break;
                            
            case 'r': 
                // currently only used for parameter substitution
                // will be extended in the future
                dryrun = true;
                break;

            case 't':
            	optimizerRules.add(opts.getValStr());
                break;
                            
            case 'v':
                properties.setProperty(VERBOSE, ""+true);
                verbose = true;
                break;

            case 'w':
                properties.setProperty("aggregate.warning", ""+false);
                break;

            case 'x':
                try {
                    execType = PigServer.parseExecType(opts.getValStr());
                    } catch (IOException e) {
                        throw new RuntimeException("ERROR: Unrecognized exectype.", e);
                    }
                break;
                
            case 'P':
            {
                InputStream inputStream = null;
                try {
                    FileLocalizer.FetchFileRet localFileRet = FileLocalizer.fetchFile(properties, opts.getValStr());
                    inputStream = new BufferedInputStream(new FileInputStream(localFileRet.file));
                    properties.load(inputStream) ;
                } catch (IOException e) {
                    throw new RuntimeException("Unable to parse properties file '" + opts.getValStr() + "'");
                } finally {
                    if (inputStream != null) {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                        } 
                    }
                }
            }
            break;
            
            default: {
                Character cc = Character.valueOf(opt);
                throw new AssertionError("Unhandled option " + cc.toString());
                     }
            }
        }
        // create the context with the parameter
        PigContext pigContext = new PigContext(execType, properties);
        
        // create the static script state object
        String commandLine = LoadFunc.join((AbstractList<String>)Arrays.asList(args), " ");
        ScriptState scriptState = ScriptState.start(commandLine);
        if (listener != null) {
            scriptState.registerListener(listener);
        }
        

        if(logFileName == null && !userSpecifiedLog) {
            logFileName = validateLogFile(properties.getProperty("pig.logfile"), null);
        }
        
        pigContext.getProperties().setProperty("pig.logfile", (logFileName == null? "": logFileName));
     
        // configure logging
        configureLog4J(properties, pigContext);
        
        if(logFileName != null) {
            log.info("Logging error messages to: " + logFileName);
        }
        
        if(optimizerRules.size() > 0) {
        	pigContext.getProperties().setProperty("pig.optimizer.rules", ObjectSerializer.serialize(optimizerRules));
        }
        
        if (properties.get("udf.import.list")!=null)
            PigContext.initializeImportList((String)properties.get("udf.import.list"));

        LogicalPlanBuilder.classloader = pigContext.createCl(null);

        // construct the parameter substitution preprocessor
        Grunt grunt = null;
        BufferedReader in;
        String substFile = null;
        switch (mode) {
        case FILE: {
            FileLocalizer.FetchFileRet localFileRet = FileLocalizer.fetchFile(properties, file);
            if (localFileRet.didFetch) {
                properties.setProperty("pig.jars.relative.to.dfs", "true");
            }
            in = new BufferedReader(new FileReader(localFileRet.file));

            // run parameter substitution preprocessor first
            substFile = file + ".substituted";
            pin = runParamPreprocessor(properties, in, params, paramFiles, substFile, debug || dryrun || checkScriptOnly);
            if (dryrun) {
                log.info("Dry run completed. Substituted pig script is at " + substFile);
                return ReturnCode.SUCCESS;
            }

            logFileName = validateLogFile(logFileName, file);
            pigContext.getProperties().setProperty("pig.logfile", logFileName);

            // Set job name based on name of the script
            pigContext.getProperties().setProperty(PigContext.JOB_NAME, 
                                                   "PigLatin:" +new File(file).getName()
            );
            
            if (!debug) {
                new File(substFile).deleteOnExit();
            }
            
            scriptState.setScript(new File(file));
            
            grunt = new Grunt(pin, pigContext);
            gruntCalled = true;
            
            if(checkScriptOnly) {
                grunt.checkScript(substFile);
                System.err.println(file + " syntax OK");
                rc = ReturnCode.SUCCESS;
            } else {
                int results[] = grunt.exec();
                rc = getReturnCodeForStats(results);
            }
            
            return rc;
        }

        case STRING: {
            if(checkScriptOnly) {
                System.err.println("ERROR:" +
                        "-c (-check) option is only valid " +
                        "when executing pig with a pig script file)");
                return ReturnCode.ILLEGAL_ARGS;
            }
            // Gather up all the remaining arguments into a string and pass them into
            // grunt.
            StringBuffer sb = new StringBuffer();
            String remainders[] = opts.getRemainingArgs();
            for (int i = 0; i < remainders.length; i++) {
                if (i != 0) sb.append(' ');
                sb.append(remainders[i]);
            }
            
            scriptState.setScript(sb.toString());
            
            in = new BufferedReader(new StringReader(sb.toString()));
            grunt = new Grunt(in, pigContext);
            gruntCalled = true;
            int results[] = grunt.exec();
            return getReturnCodeForStats(results);
        }

        default:
            break;
        }

        // If we're here, we don't know yet what they want.  They may have just
        // given us a jar to execute, they might have given us a pig script to
        // execute, or they might have given us a dash (or nothing) which means to
        // run grunt interactive.
        String remainders[] = opts.getRemainingArgs();
        if (remainders == null) {
            if(checkScriptOnly) {
                System.err.println("ERROR:" +
                        "-c (-check) option is only valid " +
                        "when executing pig with a pig script file)");
                return ReturnCode.ILLEGAL_ARGS;
            }
            // Interactive
            mode = ExecMode.SHELL;
            ConsoleReader reader = new ConsoleReader(System.in, new OutputStreamWriter(System.out));
            reader.setDefaultPrompt("grunt> ");
            final String HISTORYFILE = ".pig_history";
            String historyFile = System.getProperty("user.home") + File.separator  + HISTORYFILE;
            reader.setHistory(new History(new File(historyFile)));
            ConsoleReaderInputStream inputStream = new ConsoleReaderInputStream(reader);
            grunt = new Grunt(new BufferedReader(new InputStreamReader(inputStream)), pigContext);
            grunt.setConsoleReader(reader);
            gruntCalled = true;
            grunt.run();
            return ReturnCode.SUCCESS;
        } else {
            // They have a pig script they want us to run.
            if (remainders.length > 1) {
                   throw new RuntimeException("Encountered unexpected arguments on command line - please check the command line.");
            }
            mode = ExecMode.FILE;
            
            FileLocalizer.FetchFileRet localFileRet = FileLocalizer.fetchFile(properties, remainders[0]);
            if (localFileRet.didFetch) {
                properties.setProperty("pig.jars.relative.to.dfs", "true");
            }
            in = new BufferedReader(new FileReader(localFileRet.file));

            // run parameter substitution preprocessor first
            substFile = remainders[0] + ".substituted";
            pin = runParamPreprocessor(properties, in, params, paramFiles, substFile, debug || dryrun || checkScriptOnly);
            if (dryrun){
                log.info("Dry run completed. Substituted pig script is at " + substFile);
                return ReturnCode.SUCCESS;
            }
            
            logFileName = validateLogFile(logFileName, remainders[0]);
            pigContext.getProperties().setProperty("pig.logfile", logFileName);

            if (!debug) {
                new File(substFile).deleteOnExit();
            }

            // Set job name based on name of the script
            pigContext.getProperties().setProperty(PigContext.JOB_NAME, 
                                                   "PigLatin:" +new File(remainders[0]).getName()
            );

            scriptState.setScript(localFileRet.file);
            
            grunt = new Grunt(pin, pigContext);
            gruntCalled = true;
            
            if(checkScriptOnly) {
                grunt.checkScript(substFile);
                System.err.println(remainders[0] + " syntax OK");
                rc = ReturnCode.SUCCESS;
            } else {
                int results[] = grunt.exec();
                rc = getReturnCodeForStats(results);
            }
            return rc;
        }

        // Per Utkarsh and Chris invocation of jar file via pig depricated.
    } catch (ParseException e) {
        usage();
        rc = ReturnCode.PARSE_EXCEPTION;
        PigStatsUtil.setErrorMessage(e.getMessage());
    } catch (org.apache.pig.tools.parameters.ParseException e) {
        usage();
        rc = ReturnCode.PARSE_EXCEPTION;
        PigStatsUtil.setErrorMessage(e.getMessage());
    } catch (IOException e) {
        if (e instanceof PigException) {
            PigException pe = (PigException)e;
            rc = (pe.retriable()) ? ReturnCode.RETRIABLE_EXCEPTION 
                    : ReturnCode.PIG_EXCEPTION;
            PigStatsUtil.setErrorMessage(pe.getMessage());
            PigStatsUtil.setErrorCode(pe.getErrorCode());
        } else {
            rc = ReturnCode.IO_EXCEPTION;
            PigStatsUtil.setErrorMessage(e.getMessage());
        }
        
        if(!gruntCalled) {
        	LogUtils.writeLog(e, logFileName, log, verbose, "Error before Pig is launched");
        }
    } catch (Throwable e) {
        rc = ReturnCode.THROWABLE_EXCEPTION;
        PigStatsUtil.setErrorMessage(e.getMessage());
        if(!gruntCalled) {
        	LogUtils.writeLog(e, logFileName, log, verbose, "Error before Pig is launched");
        }      
    } finally {
        // clear temp files
        FileLocalizer.deleteTempFiles();
        PerformanceTimerFactory.getPerfTimerFactory().dumpTimers();        
    }
    
    return rc;
}

private static int getReturnCodeForStats(int[] stats) {
    return (stats[1] == 0) ? ReturnCode.SUCCESS         // no failed jobs
                : (stats[0] == 0) ? ReturnCode.FAILURE  // no succeeded jobs
                        : ReturnCode.PARTIAL_FAILURE;   // some jobs have failed
}

//TODO jz: log4j.properties should be used instead
private static void configureLog4J(Properties properties, PigContext pigContext) {
    // TODO Add a file appender for the logs
    // TODO Need to create a property in the properties file for it.
    // sgroschupf, 25Feb2008: this method will be obsolete with PIG-115.
     
    String log4jconf = properties.getProperty(LOG4J_CONF);
    String trueString = "true";
    boolean brief = trueString.equalsIgnoreCase(properties.getProperty(BRIEF));
    Level logLevel = Level.INFO;

    String logLevelString = properties.getProperty(DEBUG);
    if (logLevelString != null){
        logLevel = Level.toLevel(logLevelString, Level.INFO);
    }
    
    Properties props = new Properties();
    FileReader propertyReader = null;
    if (log4jconf != null) {
        try {
            propertyReader = new FileReader(log4jconf);
            props.load(propertyReader);
        }
        catch (IOException e)
        {
            System.err.println("Warn: Cannot open log4j properties file, use default");
        }
        finally
        {
            if (propertyReader != null) try {propertyReader.close();} catch(Exception e) {}
        }
    }
    if (props.size() == 0) {
        props.setProperty("log4j.logger.org.apache.pig", logLevel.toString());
        if((logLevelString = System.getProperty("pig.logfile.level")) == null){
            props.setProperty("log4j.rootLogger", "INFO, PIGCONSOLE");
        }
        else{
            logLevel = Level.toLevel(logLevelString, Level.INFO);
            props.setProperty("log4j.logger.org.apache.pig", logLevel.toString());
            props.setProperty("log4j.rootLogger", "INFO, PIGCONSOLE, F");
            props.setProperty("log4j.appender.F","org.apache.log4j.RollingFileAppender");
            props.setProperty("log4j.appender.F.File",properties.getProperty("pig.logfile"));
            props.setProperty("log4j.appender.F.layout","org.apache.log4j.PatternLayout");
            props.setProperty("log4j.appender.F.layout.ConversionPattern", brief ? "%m%n" : "%d [%t] %-5p %c - %m%n");
        }
        
        props.setProperty("log4j.appender.PIGCONSOLE","org.apache.log4j.ConsoleAppender");    
        props.setProperty("log4j.appender.PIGCONSOLE.target", "System.err");        
        props.setProperty("log4j.appender.PIGCONSOLE.layout","org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.PIGCONSOLE.layout.ConversionPattern", brief ? "%m%n" : "%d [%t] %-5p %c - %m%n");
    }

    PropertyConfigurator.configure(props);
    logLevel = Logger.getLogger("org.apache.pig").getLevel();
    Properties backendProps = pigContext.getLog4jProperties();
    backendProps.setProperty("log4j.logger.org.apache.pig.level", logLevel.toString());
    pigContext.setLog4jProperties(backendProps);
    pigContext.setDefaultLogLevel(logLevel);
}
 
// returns the stream of final pig script to be passed to Grunt
private static BufferedReader runParamPreprocessor(Properties properties, BufferedReader origPigScript, ArrayList<String> params,
                                            ArrayList<String> paramFiles, String scriptFile, boolean createFile) 
                                throws org.apache.pig.tools.parameters.ParseException, IOException{
    
    ArrayList<String> paramFiles2 = new ArrayList<String>();
    for (String param: paramFiles) {
        FileLocalizer.FetchFileRet localFileRet = FileLocalizer.fetchFile(properties, param);
        paramFiles2.add(localFileRet.file.getAbsolutePath());
    }    
    
    ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(50);
    String[] type1 = new String[1];
    String[] type2 = new String[1];

    if (createFile){
        BufferedWriter fw = new BufferedWriter(new FileWriter(scriptFile));
        psp.genSubstitutedFile (origPigScript, fw, params.size() > 0 ? params.toArray(type1) : null, 
                                paramFiles.size() > 0 ? paramFiles2.toArray(type2) : null);
        return new BufferedReader(new FileReader (scriptFile));

    } else {
        StringWriter writer = new StringWriter();
        psp.genSubstitutedFile (origPigScript, writer,  params.size() > 0 ? params.toArray(type1) : null, 
                                paramFiles.size() > 0 ? paramFiles2.toArray(type2) : null);
        return new BufferedReader(new StringReader(writer.toString()));
    }
}
    
private static String getVersionString() {
	String findContainingJar = JarManager.findContainingJar(Main.class);
	  try { 
          JarFile jar = new JarFile(findContainingJar); 
          final Manifest manifest = jar.getManifest(); 
          final Map <String,Attributes> attrs = manifest.getEntries(); 
          Attributes attr = attrs.get("org/apache/pig");
          String version = attr.getValue("Implementation-Version");
          String svnRevision = attr.getValue("Svn-Revision");
          String buildTime = attr.getValue("Build-TimeStamp");
          // we use a version string similar to svn 
          //svn, version 1.4.4 (r25188)
          // compiled Sep 23 2007, 22:32:34
          return "Apache Pig version " + version + " (r" + svnRevision + ") \ncompiled "+buildTime;
      } catch (Exception e) { 
          throw new RuntimeException("unable to read pigs manifest file", e); 
      } 
}

/**
 * Print usage string.
 */
public static void usage()
{
	System.out.println("\n"+getVersionString()+"\n");
        System.out.println("USAGE: Pig [options] [-] : Run interactively in grunt shell.");
        System.out.println("       Pig [options] -e[xecute] cmd [cmd ...] : Run cmd(s).");
        System.out.println("       Pig [options] [-f[ile]] file : Run cmds found in file.");
        System.out.println("  options include:");
        System.out.println("    -4, -log4jconf - Log4j configuration file, overrides log conf");
        System.out.println("    -b, -brief - Brief logging (no timestamps)");
        System.out.println("    -c, -check - Syntax check");
        System.out.println("    -d, -debug - Debug level, INFO is default");
        System.out.println("    -e, -execute - Commands to execute (within quotes)");
        System.out.println("    -f, -file - Path to the script to execute");
        System.out.println("    -h, -help - Display this message. You can specify topic to get help for that topic.");
	System.out.println("        properties is the only topic currently supported: -h properties.");
        System.out.println("    -i, -version - Display version information");
        System.out.println("    -l, -logfile - Path to client side log file; default is current working directory.");
        System.out.println("    -m, -param_file - Path to the parameter file");
        System.out.println("    -p, -param - Key value pair of the form param=val");
        System.out.println("    -r, -dryrun - Produces script with substituted parameters. Script is not executed.");
        System.out.println("    -t, -optimizer_off - Turn optimizations off. The following values are supported:");
        System.out.println("            SplitFilter - Split filter conditions");
        System.out.println("            MergeFilter - Merge filter conditions");
        System.out.println("            PushUpFilter - Filter as early as possible");
        System.out.println("            PushDownForeachFlatten - Join or explode as late as possible");
        System.out.println("            ColumnMapKeyPrune - Remove unused data");
        System.out.println("            LimitOptimizer - Limit as early as possible");
        System.out.println("            AddForEach - Add ForEach to remove unneeded columns");
        System.out.println("            MergeForEach - Merge adjacent ForEach");
        System.out.println("            LogicalExpressionSimplifier - Combine multiple expressions");
        System.out.println("            All - Disable all optimizations");
        System.out.println("        All optimizations are enabled by default. Optimization values are case insensitive.");
        System.out.println("    -v, -verbose - Print all error messages to screen");
        System.out.println("    -w, -warning - Turn warning logging on; also turns warning aggregation off");
        System.out.println("    -x, -exectype - Set execution mode: local|mapreduce, default is mapreduce.");
        System.out.println("    -F, -stop_on_failure - Aborts execution on the first failed job; default is off");
        System.out.println("    -M, -no_multiquery - Turn multiquery optimization off; default is on");
        System.out.println("    -P, -propertyFile - Path to property file");
}

public static void printProperties(){
	System.out.println("The following properties are supported:");
	System.out.println("    Logging:");
        System.out.println("        verbose=true|false; default is false. This property is the same as -v switch");    
        System.out.println("        brief=true|false; default is false. This property is the same as -b switch");
        System.out.println("        debug=OFF|ERROR|WARN|INFO|DEBUG; default is INFO. This property is the same as -d switch");
        System.out.println("        aggregate.warning=true|false; default is true. If true, prints count of warnings");
        System.out.println("            of each type rather than logging each warning.");
        System.out.println("    Performance tuning:");
        System.out.println("        pig.cachedbag.memusage=<mem fraction>; default is 0.1 (10% of all memory).");
	System.out.println("            Note that this memory is shared across all large bags used by the application.");     	
        System.out.println("        pig.skewedjoin.reduce.memusagea=<mem fraction>; default is 0.3 (30% of all memory).");
        System.out.println("            Specifies the fraction of heap available for the reducer to perform the join.");
        System.out.println("        pig.exec.nocombiner=true|false; default is false. ");
        System.out.println("            Only disable combiner as a temporary workaround for problems.");
        System.out.println("        opt.multiquery=true|false; multiquery is on by default.");
        System.out.println("            Only disable multiquery as a temporary workaround for problems.");
        System.out.println("        pig.tmpfilecompression=true|false; compression is off by default.");
        System.out.println("            Determines whether output of intermediate jobs is compressed.");
        System.out.println("        pig.tmpfilecompression.codec=lzo|gzip; default is gzip.");
        System.out.println("            Used in conjunction with pig.tmpfilecompression. Defines compression type."); 
        System.out.println("        pig.noSplitCombination=true|false. Split combination is on by default.");
        System.out.println("            Determines if multiple small files are combined into a single map.");
        System.out.println("    Miscellaneous:");
        System.out.println("        exectype=mapreduce|local; default is mapreduce. This property is the same as -x switch");
        System.out.println("        pig.additional.jars=<comma seperated list of jars>. Used in place of register command.");
        System.out.println("        udf.import.list=<comma seperated list of imports>. Used to avoid package names in UDF.");
        System.out.println("        stop.on.failure=true|false; default is false. Set to true to terminate on the first error.");
	System.out.println("Additionally, any Hadoop property can be specified.");
}

private static String validateLogFile(String logFileName, String scriptName) {
    String strippedDownScriptName = null;
    
    if(scriptName != null) {
        File scriptFile = new File(scriptName);
        if(!scriptFile.isDirectory()) {
            String scriptFileAbsPath;
            try {
                scriptFileAbsPath = scriptFile.getCanonicalPath();
            } catch (IOException ioe) {
                log.warn("Could not compute canonical path to the script file " + ioe.getMessage());
                return null;
            }            
            strippedDownScriptName = getFileFromCanonicalPath(scriptFileAbsPath);
        }
    }
    
    String defaultLogFileName = (strippedDownScriptName == null ? "pig_" : strippedDownScriptName) + new Date().getTime() + ".log";
    File logFile;    
    
    if(logFileName != null) {
        logFile = new File(logFileName);
    
        //Check if the file name is a directory 
        //append the default file name to the file
        if(logFile.isDirectory()) {            
            if(logFile.canWrite()) {
                try {
                    logFileName = logFile.getCanonicalPath() + File.separator + defaultLogFileName;
                } catch (IOException ioe) {
                    log.warn("Could not compute canonical path to the log file " + ioe.getMessage());
                    return null;
                }
                return logFileName;
            } else {
                log.warn("Need write permission in the directory: " + logFileName + " to create log file.");
                return null;
            }
        } else {
            //we have a relative path or an absolute path to the log file
            //check if we can write to the directory where this file is/will be stored
            
            if (logFile.exists()) {
                if(logFile.canWrite()) {
                    try {
                        logFileName = new File(logFileName).getCanonicalPath();
                    } catch (IOException ioe) {
                        log.warn("Could not compute canonical path to the log file " + ioe.getMessage());
                        return null;
                    }
                    return logFileName;
                } else {
                    //do not have write permissions for the log file
                    //bail out with an error message
                    log.warn("Cannot write to file: " + logFileName + ". Need write permission.");
                    return logFileName;
                }
            } else {
                logFile = logFile.getParentFile();
                
                if(logFile != null) {
                    //if the directory is writable we are good to go
                    if(logFile.canWrite()) {
                        try {
                            logFileName = new File(logFileName).getCanonicalPath();
                        } catch (IOException ioe) {
                            log.warn("Could not compute canonical path to the log file " + ioe.getMessage());
                            return null;
                        }
                        return logFileName;
                    } else {
                        log.warn("Need write permission in the directory: " + logFile + " to create log file.");
                        return logFileName;
                    }
                }//end if logFile != null else is the default in fall through                
            }//end else part of logFile.exists()
        }//end else part of logFile.isDirectory()
    }//end if logFileName != null
    
    //file name is null or its in the current working directory 
    //revert to the current working directory
    String currDir = System.getProperty("user.dir");
    logFile = new File(currDir);
    logFileName = currDir + File.separator + (logFileName == null? defaultLogFileName : logFileName);
    if(logFile.canWrite()) {        
        return logFileName;
    }    
    log.warn("Cannot write to log file: " + logFileName);
    return null;
}

private static String getFileFromCanonicalPath(String canonicalPath) {
    return canonicalPath.substring(canonicalPath.lastIndexOf(File.separator));
}

}
