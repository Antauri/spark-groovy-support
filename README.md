# Support for Groovy scripts in a distributed Spark cluster
The fix to SPARK-2171 for the (Groovy + Spark) combo in a distributed Spark cluster (see SPARK-15582 discussion about scripting support in Spark).

# History
Researching a way to provide Groovy scripting support to fellow colleagues, we've stumbled upon SPARK-2171 which argues that there's nothing to do regarding Groovy and Spark, as it's directly compatible to the Spark M/R execution model. On closer inspection, the original SPARK-2171 test was done in a local[] VM, which means that the code defined in the closures already existed in the JVM. However, running in a distributed cluster, your average Groovy script that tried to to Spark M/R would fail with different ClassNotFoundExceptions (because of the missing closures) or Task Not Serializable (by not extending a SerializableScript base).

# Why
We had a Spring-based application (the driver). We had a Spark cluster. We needed a way to connect the two worlds and make writing one-shot analytic scripts extremelly easy. Now imagine for a second what you can do with the full power of Spring + Groovy + Spark. Got your attention? Good! That's why! Carry on reading :D

# The fix/work-around
In reality, in a distributed Spark cluster, you need to make your code reach the executor nodes somehow. To do that, a few requirements are needed:
- you need some kind of "Spring In Spark" support (see the "spring-spark" fork of the original project, the original project was retired by the original author). This allows you to use @ Autowired inside the Groovy-defined classes/closures in the scripts. If you don't use Spring, you can skip this step, but the example code below assumes a Spring context (note the RequestMapping annotations);
- you intercept the script before execution;
- you compile it, save it to a JAR, use Spark's addJar method before launching the script in the driver context;
- script runs, returns two keys in JSON "output" (from println*, etc.) and "return" (last object in script or specific return stmt); 
- in case of exception it just returns "exception" (single) with the stacktrace;
- profit!

The method below could (in theory) be extended to any other kind of scripting language which can provide a compilation to byte[]-code path. Groovy was (in all honesty) the simplest one to do. We are still researching support for other languages, yet implementations may be possible for:
- Janino;
- Kotlin;
- Ceylon;
- Javascript (see Eclair-JS @ https://github.com/EclairJS/eclairjs-node running over Nashorn);
- Jython;

```
package net.somewhere.to.your.package;

// Imports
import java.io.Serializable;
import org.joda.time.DateTime;
import groovy.lang.Script;

/**
 * Setting this as a base-class, for the GroovyShell to
 * pick-up and use as a serializable;
 */
public abstract class SerializableScript extends Script implements Serializable {

    // Privates
    private static final long serialVersionUID = 6799589283932151864L;
    
    /**
     * Method is used to hide the implementation details
     * on how to generate the name of the script. We currently
     * generate it from the millis of the current execution
     * time, but some other implementation may be put here;
     */
    public String getScriptName () {
        // Return the script name as the current time
        Long dateOfNow = DateTime.now ().getMillis ();
        return String.format ("ScriptOf%d", dateOfNow);
    }
}
```

```
package net.somewhere.to.your.package;

// Imports
// Make sure to resolve all your imports from below;

/**
 * Job is used to received a Groovy-language script that
 * is going to be compiled and executed inside of this JVM to
 * which we pass it the Spark service so that any kind of
 * analytic code can be executed against it;
 */
@ CrossOrigin
@ RestController
@ RequestMapping ("job/Groovy")
public class Groovy implements Serializable {

    // Privates
    private static final long serialVersionUID = 5608151611731294203L;
    private static final CompilerConfiguration compilerConfiguration = new CompilerConfiguration ();
    
    // Dependency injected
    @ Autowired private SparkService sparkService;
    
    @ PostConstruct
    public void initiateConfiguration () {
        // Set the base-script class of any of our executing scripts
        compilerConfiguration.setScriptBaseClass ("net.somewhere.to.your.package.SerializableScript");
        compilerConfiguration.setTargetBytecode (CompilerConfiguration.JDK8);
        compilerConfiguration.setDebug (false);
    }

    /**
     * Method takes care of preparing a few requirements for the job
     * while also setting the try/catch block for the given job, due
     * to the fact that the Groovy scripts may fail compilation or 
     * generate a few exceptions we don't want to be thrown above;
     */ 
    private Map<String, Object> prepareJob (final String sourceCode) 
        throws IllegalArgumentException, IOException {

        // !!
        try {
            // Return
            return prepareScript (sourceCode);
        } catch (final Exception e) {
            // Caught
            return ImmutableMap.of ("exception", 
                ExceptionUtils.getStackTrace (e));
        }
    }

    /**
     * Prepares a few script requirements required, for example, the script
     * start and stop times, binding values, intercepter and more;
     * @param sourceCode
     * @return
     * @throws IOException
     */
    private Map<String, Object> prepareScript (String sourceCode) throws IOException {
        // Defaults
        Map<String, Object> toReturn = new HashMap<String, Object> ();
        IntercepterClojure outputCollector = new IntercepterClojure (this);
        SystemOutputInterceptor systemOutputInterceptor = new SystemOutputInterceptor (outputCollector, true);
        systemOutputInterceptor.start ();

        // !!
        try {
            // Bindings
            HashMap<String, Object> bindingValues = new HashMap<String, Object> ();
            bindingValues.put ("sparkService", sparkService);
            
            // Process
            toReturn.put ("return", 
                evaluateScript (sourceCode, 
                    bindingValues));
        } catch (final Exception e) {
            // Caught
            return ImmutableMap.of ("exception", 
                ExceptionUtils.getStackTrace (e));
        } finally {
            // Clean-up after ourselves
            systemOutputInterceptor.stop ();
            systemOutputInterceptor.close ();
        }

        // Put the output as it is (objects)
        toReturn.put ("output", outputCollector.getOutput ());
        return toReturn;
    }

    /**
     * Instantiates a GroovyShell and runs the script from
     * in-memory, without any other requirements;
     * @param sourceCode
     * @param bindingValues
     * @return
     * @throws IOException 
     * @throws CompilationFailedException 
     * @throws IllegalStateException 
     * @throws ClassNotFoundException 
     */
    private String evaluateScript (String sourceCode, HashMap<String, Object> bindingValues) 
        throws CompilationFailedException, IOException, IllegalStateException, ClassNotFoundException {
        
        // Compile it
        Long dateOfNow = DateTime.now ().getMillis ();
        String nameOfScript = String.format ("ScriptOf%d", dateOfNow);
        String groovyCommonPath = new String ("/path/to/your/distributed-fs/groovy-scripts");
        String pathOfJar = String.format ("%s/%s.jar", groovyCommonPath, String.format ("JarOf%d", dateOfNow));
        File archiveFile = new File (pathOfJar);
        Files.createParentDirs (archiveFile);
        
        // With resources
        List<?> compilationList = compileGroovyScript (nameOfScript, sourceCode);
        try (JarArchiveOutputStream oneJobJar = new JarArchiveOutputStream (new FileOutputStream (new File (pathOfJar)))) {
            // For
            for (Object compileClass : compilationList) {
                // Append
                GroovyClass groovyClass = (GroovyClass) compileClass;
                JarArchiveEntry oneJarEntry = new JarArchiveEntry (String.format ("%s.class", groovyClass.getName ()));
                oneJarEntry.setSize (groovyClass.getBytes ().length);
                byte[] bytecodeOfClass = groovyClass.getBytes ();
                oneJobJar.putArchiveEntry (oneJarEntry);
                oneJobJar.write (bytecodeOfClass);
                oneJobJar.closeArchiveEntry ();
            }
            
            // End it up
            oneJobJar.finish ();
            oneJobJar.close ();
        }
        
        // Append the JAR to the execution environment
        sparkService.getSparkContext ().addJar (pathOfJar);
        
        // Prepare the shell & loader, parse, auto-wire and run the script
        GroovyShell groovyShell = new GroovyShell (new Binding (bindingValues), compilerConfiguration);
        Script parsedScript = groovyShell.parse (sourceCode, nameOfScript);
        SparkService.autowire (parsedScript);
        Object returnOfScript = parsedScript.run ();
        
        // Return with specific check
        return returnOfScript != null  ? 
            returnOfScript.toString () : 
                new String ("Empty");
    }
    
    /**
     * Method used to compile the given script source code into
     * byte-code so that we can package this into a JAR and add it
     * to the current SparkContext;
     * @param className
     * @param theScript
     * @return
     */
    public List<?> compileGroovyScript (final String className, final String theScript) {
        // Prepare it
        CompilationUnit compileUnit = new CompilationUnit (compilerConfiguration);
        compileUnit.addSource (className, theScript);
        compileUnit.compile (Phases.CLASS_GENERATION);
        return compileUnit.getClasses ();
    }

    /**
     * Method receives the script and sends it to the implementation,
     * being just a method that respects the JobInterface, but instead of
     * a day (or other type of parameters) it catches the source code of
     * the POST'ed Groovy script;
     */
    @ Override
    @ RequestMapping (value = "executeScript", method = RequestMethod.POST)
    public @ ResponseBody Map<String, Object> submitJob (@ RequestBody String sourceCode) 
        throws IllegalArgumentException, IOException {

        // Return
        return prepareJob (sourceCode);
    }
    
    /**
     * Method allows for running with CORS support but with custom headers
     * added by us manually (for cases where the CorsOrigins annotation fails
     * to work out of the box);
     * @param sourceCode
     * @param oneResponse
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @ RequestMapping (value = "executeScriptCorsSupport", method = RequestMethod.POST)
    public @ ResponseBody Map<String, Object> submitJobWithCorsSupport (@ RequestBody String sourceCode, HttpServletResponse oneResponse) 
        throws IllegalArgumentException, IOException {
        
        // Append'em
        oneResponse.addHeader("Access-Control-Allow-Origin", "*");
        oneResponse.addHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE");
        oneResponse.addHeader("Access-Control-Allow-Headers", "Content-Type");
        oneResponse.addHeader("Access-Control-Max-Age", "3600");
        
        // Return
        return submitJob (sourceCode);
    }
}

/**
 * Output collector implementation, based on using
 * a StringBuffer to get back the contents;
 */
class IntercepterClojure extends Closure<Object> {

    // Privates
    private static final long serialVersionUID = -8753771526714601423L;
    private Set<Object> stringBuffer = new LinkedHashSet <Object> ();

    public IntercepterClojure (Object owner) {
        super (owner);
    }

    @ Override
    public Object call (Object params) {
        // Append
        stringBuffer.add (params);
        return false;
    }

    @ Override
    public Object call (Object... args) {
        // Go
        for (Object arg : args) {
            // Append
            stringBuffer.add (arg);
        }
        
        // Return
        return false;
    }

    public Set<Object> getOutput () {
        // Return
        return this.stringBuffer;
    }
}
```
