# 前言

作为数据工程师，你可能会碰到过很多种启动PySpark的方法，可能搞不懂这些方法有什么共同点、有什么区别，不同的方法对程序开发、部署有什么影响，今天我们一起分析一下这些启动PySpark的方法。

以下代码分析都是基于**spark-2.4.4**版本展开的，为了避免歧义，务必对照这个版本的Spark深入理解。


# 启动PySpark的方法

序号 | 方法 | 说明
---|---|---
1 | /path/to/spark-submit python_file.py | 最常用的方式，可以通过spark-submit指定Spark参数，通常用于投产
2 | /path/to/python python_file.py | Spark Session启动代码和参数写在python_file代码里，也可以用于投产；如果不指定python_file，在jupyter环境里，用户可以手动启动Spark Session，也可以实现交互式编程，用户可以灵活地在代码里指定Spark参数，更加方便
3 | /path/to/pyspark | 这种方式用于交互式编程，比如在jupyter notebook里利用PySpark探索数据，可以方便的和jupyter集成，启动jupyter kernel的时候自动启动Spark Session，用户不需要再手动启动Spark Session，降低门槛，但对资深用户不友好


# 启动PySpark代码分析

下面我们分别来分析一下三种方法的代码实现过程。

## /path/to/spark-submit python_file.py

```
graph TD
A(1 shell: spark-submit)-->B(2 shell: spark-class org.apache.spark.deploy.SparkSubmit python_file.py)
B-->C(3 jvm: org.apache.spark.launcher.Main org.apache.spark.deploy.SparkSubmit python_file.py)
C-->D(4 scala: org.apache.spark.launcher.Main)
D-->E(5 jvm: org.apache.spark.deploy.SparkSubmit python_file.py)
E-->F(6 scala: org.apache.spark.deploy.SparkSubmit)
F-->G(7 scala: org.apache.spark.deploy.PythonRunner)
G-->H(8 scala: py4j.GatewayServer.start)
H-->I(9 scala: start a new process running /path/to/python python_file.py)
```

1. spark-submit是一个shell脚本
2. spark-submit调用shell命令spark-class org.apache.spark.deploy.SparkSubmit python_file.py
3. spark-class，line 71，执行jvm org.apache.spark.launcher.Main org.apache.spark.deploy.SparkSubmit python_file.py重写SparkSubmit参数
```
# The launcher library will print arguments separated by a NULL character, to allow arguments with
# characters that would be otherwise interpreted by the shell. Read that in a while loop, populating
# an array that will be used to exec the final command.
#
# The exit code of the launcher is appended to the output, so the parent shell removes it from the
# command array and checks the value to see if the launcher succeeded.
build_command() {
  "$RUNNER" -Xmx128m -cp "$LAUNCH_CLASSPATH" org.apache.spark.launcher.Main "$@"
  printf "%d\0" $?
}

# Turn off posix mode since it does not allow process substitution
set +o posix
CMD=()
while IFS= read -d '' -r ARG; do
  CMD+=("$ARG")
done < <(build_command "$@")
```
4. 深入分析一下org.apache.spark.launcher.Main如何重写SparkSubmit参数，可以看到buildCommand分三种情况，分别对应三种不同的场景，PySpark shell、Spark R shell、Spark submit，场景对用不同的class
```
 /**
   * This constructor is used when invoking spark-submit; it parses and validates arguments
   * provided by the user on the command line.
   */
  SparkSubmitCommandBuilder(List<String> args) {
    this.allowsMixedArguments = false;
    this.parsedArgs = new ArrayList<>();
    boolean isExample = false;
    List<String> submitArgs = args;
    this.userArgs = Collections.emptyList();

    if (args.size() > 0) {
      switch (args.get(0)) {
        case PYSPARK_SHELL:
          this.allowsMixedArguments = true;
          appResource = PYSPARK_SHELL;
          submitArgs = args.subList(1, args.size());
          break;

        case SPARKR_SHELL:
          this.allowsMixedArguments = true;
          appResource = SPARKR_SHELL;
          submitArgs = args.subList(1, args.size());
          break;

        case RUN_EXAMPLE:
          isExample = true;
          appResource = SparkLauncher.NO_RESOURCE;
          submitArgs = args.subList(1, args.size());
      }

      this.isExample = isExample;
      OptionParser parser = new OptionParser(true);
      parser.parse(submitArgs);
      this.isSpecialCommand = parser.isSpecialCommand;
    } else {
      this.isExample = isExample;
      this.isSpecialCommand = true;
    }
  }

  @Override
  public List<String> buildCommand(Map<String, String> env)
      throws IOException, IllegalArgumentException {
    if (PYSPARK_SHELL.equals(appResource) && !isSpecialCommand) {
      return buildPySparkShellCommand(env);
    } else if (SPARKR_SHELL.equals(appResource) && !isSpecialCommand) {
      return buildSparkRCommand(env);
    } else {
      return buildSparkSubmitCommand(env);
    }
  }
```
5. 这里buildCommand返回的class是org.apache.spark.deploy.SparkSubmit，参数是python_file.py
6. 因为SparkSubmit的参数是.py文件，所以选择class org.apache.spark.deploy.PythonRunner

最后看一下PythonRunner的实现，首先创建一个py4j.GatewayServer的线程，用于接收python发起的请求，然后起一个子进程执行用户的python代码python_file.py，python_file.py会通过py4j发起各种Spark操作，就如上篇文章[PySpark工作原理](https://github.com/python279/spark_code_learning/blob/master/pyspark工作原理.md)提到的。
```
/**
 * A main class used to launch Python applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
object PythonRunner {
  def main(args: Array[String]) {
    val pythonFile = args(0)
    val pyFiles = args(1)
    val otherArgs = args.slice(2, args.length)
    val sparkConf = new SparkConf()
    val secret = Utils.createSecret(sparkConf)
    val pythonExec = sparkConf.get(PYSPARK_DRIVER_PYTHON)
      .orElse(sparkConf.get(PYSPARK_PYTHON))
      .orElse(sys.env.get("PYSPARK_DRIVER_PYTHON"))
      .orElse(sys.env.get("PYSPARK_PYTHON"))
      .getOrElse("python")

    // Format python file paths before adding them to the PYTHONPATH
    val formattedPythonFile = formatPath(pythonFile)
    val formattedPyFiles = resolvePyFiles(formatPaths(pyFiles))

    // Launch a Py4J gateway server for the process to connect to; this will let it see our
    // Java system properties and such
    val localhost = InetAddress.getLoopbackAddress()
    val gatewayServer = new py4j.GatewayServer.GatewayServerBuilder()
      .authToken(secret)
      .javaPort(0)
      .javaAddress(localhost)
      .callbackClient(py4j.GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
      .build()
    val thread = new Thread(new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions {
        gatewayServer.start()
      }
    })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()

    // Wait until the gateway server has started, so that we know which port is it bound to.
    // `gatewayServer.start()` will start a new thread and run the server code there, after
    // initializing the socket, so the thread started above will end as soon as the server is
    // ready to serve connections.
    thread.join()

    // Build up a PYTHONPATH that includes the Spark assembly (where this class is), the
    // python directories in SPARK_HOME (if set), and any files in the pyFiles argument
    val pathElements = new ArrayBuffer[String]
    pathElements ++= formattedPyFiles
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    val pythonPath = PythonUtils.mergePythonPaths(pathElements: _*)

    // Launch Python process
    val builder = new ProcessBuilder((Seq(pythonExec, formattedPythonFile) ++ otherArgs).asJava)
    val env = builder.environment()
    env.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
    env.put("PYSPARK_GATEWAY_PORT", "" + gatewayServer.getListeningPort)
    env.put("PYSPARK_GATEWAY_SECRET", secret)
    // pass conf spark.pyspark.python to python process, the only way to pass info to
    // python process is through environment variable.
    sparkConf.get(PYSPARK_PYTHON).foreach(env.put("PYSPARK_PYTHON", _))
    sys.env.get("PYTHONHASHSEED").foreach(env.put("PYTHONHASHSEED", _))
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    try {
      val process = builder.start()

      new RedirectThread(process.getInputStream, System.out, "redirect output").start()

      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw new SparkUserAppException(exitCode)
      }
    } finally {
      gatewayServer.shutdown()
    }
  }
```


## /path/to/python python_file

```
graph TD
A(1 shell: python python_file)-->B(2 python: SparkContext._ensure_initialized)
B-->C(3 shell: spark-submit pyspark-shell)
C-->D(4 scala: org.apache.spark.launcher.Main)
D-->E(5 jvm: org.apache.spark.deploy.SparkSubmit pyspark-shell)
E-->F(6 scala: org.apache.spark.deploy.SparkSubmit)
F-->G(7 scala: org.apache.spark.api.python.PythonGatewayServer)
G-->H(8 scala: py4j.GatewayServer.start)
```

1. 直接执行python python_file.py
2. 调用SparkContext._ensure_initialized来初始化Spark Context（第2步），调用launch_gateway创建Spark py4j.GatewayServer实例，其实最终是起一个子进程执行spark-submit pyspark-shell（第3步）
```
    @classmethod
    def _ensure_initialized(cls, instance=None, gateway=None, conf=None):
        """
        Checks whether a SparkContext is initialized or not.
        Throws error if a SparkContext is already running.
        """
        with SparkContext._lock:
            if not SparkContext._gateway:
                SparkContext._gateway = gateway or launch_gateway(conf)
                SparkContext._jvm = SparkContext._gateway.jvm
```
```
def _launch_gateway(conf=None, insecure=False):
    """
    launch jvm gateway
    :param conf: spark configuration passed to spark-submit
    :param insecure: True to create an insecure gateway; only for testing
    :return: a JVM gateway
    """
    if insecure and os.environ.get("SPARK_TESTING", "0") != "1":
        raise ValueError("creating insecure gateways is only for testing")
    if "PYSPARK_GATEWAY_PORT" in os.environ:
        gateway_port = int(os.environ["PYSPARK_GATEWAY_PORT"])
        gateway_secret = os.environ["PYSPARK_GATEWAY_SECRET"]
    else:
        SPARK_HOME = _find_spark_home()
        # Launch the Py4j gateway using Spark's run command so that we pick up the
        # proper classpath and settings from spark-env.sh
        on_windows = platform.system() == "Windows"
        script = "./bin/spark-submit.cmd" if on_windows else "./bin/spark-submit"
        command = [os.path.join(SPARK_HOME, script)]
        if conf:
            for k, v in conf.getAll():
                command += ['--conf', '%s=%s' % (k, v)]
        submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        if os.environ.get("SPARK_TESTING"):
            submit_args = ' '.join([
                "--conf spark.ui.enabled=false",
                submit_args
            ])
        command = command + shlex.split(submit_args)

        # Create a temporary directory where the gateway server should write the connection
        # information.
        conn_info_dir = tempfile.mkdtemp()
        try:
            fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
            os.close(fd)
            os.unlink(conn_info_file)

            env = dict(os.environ)
            env["_PYSPARK_DRIVER_CONN_INFO_PATH"] = conn_info_file
            if insecure:
                env["_PYSPARK_CREATE_INSECURE_GATEWAY"] = "1"

            # Launch the Java gateway.
            # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
            if not on_windows:
                # Don't send ctrl-c / SIGINT to the Java gateway:
                def preexec_func():
                    signal.signal(signal.SIGINT, signal.SIG_IGN)
                proc = Popen(command, stdin=PIPE, preexec_fn=preexec_func, env=env)
            else:
                # preexec_fn not supported on Windows
                proc = Popen(command, stdin=PIPE, env=env)

            # Wait for the file to appear, or for the process to exit, whichever happens first.
            while not proc.poll() and not os.path.isfile(conn_info_file):
                time.sleep(0.1)

            if not os.path.isfile(conn_info_file):
                raise Exception("Java gateway process exited before sending its port number")

            with open(conn_info_file, "rb") as info:
                gateway_port = read_int(info)
                gateway_secret = UTF8Deserializer().loads(info)
        finally:
            shutil.rmtree(conn_info_dir)

        # In Windows, ensure the Java child processes do not linger after Python has exited.
        # In UNIX-based systems, the child process can kill itself on broken pipe (i.e. when
        # the parent process' stdin sends an EOF). In Windows, however, this is not possible
        # because java.lang.Process reads directly from the parent process' stdin, contending
        # with any opportunity to read an EOF from the parent. Note that this is only best
        # effort and will not take effect if the python process is violently terminated.
        if on_windows:
            # In Windows, the child process here is "spark-submit.cmd", not the JVM itself
            # (because the UNIX "exec" command is not available). This means we cannot simply
            # call proc.kill(), which kills only the "spark-submit.cmd" process but not the
            # JVMs. Instead, we use "taskkill" with the tree-kill option "/t" to terminate all
            # child processes in the tree (http://technet.microsoft.com/en-us/library/bb491009.aspx)
            def killChild():
                Popen(["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(proc.pid)])
            atexit.register(killChild)

    # Connect to the gateway
    gateway_params = GatewayParameters(port=gateway_port, auto_convert=True)
    if not insecure:
        gateway_params.auth_token = gateway_secret
    gateway = JavaGateway(gateway_parameters=gateway_params)

    # Import the classes used by PySpark
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.ml.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    # TODO(davies): move into sql
    java_import(gateway.jvm, "org.apache.spark.sql.*")
    java_import(gateway.jvm, "org.apache.spark.sql.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
    java_import(gateway.jvm, "scala.Tuple2")

    return gateway
```

接下来的过程和第一种方法类似，这回选择的class是org.apache.spark.api.python.PythonGatewayServer，我们来看一下代码，就是起一个py4j.GatewayServer，处理python端发起的请求
```
/**
 * Process that starts a Py4J GatewayServer on an ephemeral port.
 *
 * This process is launched (via SparkSubmit) by the PySpark driver (see java_gateway.py).
 */
private[spark] object PythonGatewayServer extends Logging {
  initializeLogIfNecessary(true)

  def main(args: Array[String]): Unit = {
    val secret = Utils.createSecret(new SparkConf())

    // Start a GatewayServer on an ephemeral port. Make sure the callback client is configured
    // with the same secret, in case the app needs callbacks from the JVM to the underlying
    // python processes.
    val localhost = InetAddress.getLoopbackAddress()
    val builder = new GatewayServer.GatewayServerBuilder()
      .javaPort(0)
      .javaAddress(localhost)
      .callbackClient(GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
    if (sys.env.getOrElse("_PYSPARK_CREATE_INSECURE_GATEWAY", "0") != "1") {
      builder.authToken(secret)
    } else {
      assert(sys.env.getOrElse("SPARK_TESTING", "0") == "1",
        "Creating insecure Java gateways only allowed for testing")
    }
    val gatewayServer: GatewayServer = builder.build()

    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logError("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      logDebug(s"Started PythonGatewayServer on port $boundPort")
    }

    // Communicate the connection information back to the python process by writing the
    // information in the requested file. This needs to match the read side in java_gateway.py.
    val connectionInfoPath = new File(sys.env("_PYSPARK_DRIVER_CONN_INFO_PATH"))
    val tmpPath = Files.createTempFile(connectionInfoPath.getParentFile().toPath(),
      "connection", ".info").toFile()

    val dos = new DataOutputStream(new FileOutputStream(tmpPath))
    dos.writeInt(boundPort)

    val secretBytes = secret.getBytes(UTF_8)
    dos.writeInt(secretBytes.length)
    dos.write(secretBytes, 0, secretBytes.length)
    dos.close()

    if (!tmpPath.renameTo(connectionInfoPath)) {
      logError(s"Unable to write connection information to $connectionInfoPath.")
      System.exit(1)
    }

    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (System.in.read() != -1) {
      // Do nothing
    }
    logDebug("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }
}
```

## /path/to/pyspark

```
graph TD
A(1 shell: pyspark)-->B(2 shell: spark-submit pyspark-shell-main)
B-->C(3 shell: spark-class org.apache.spark.deploy.SparkSubmit pyspark-shell-main)
C-->D(4 jvm: org.apache.spark.launcher.Main org.apache.spark.deploy.SparkSubmit pyspark-shell-main)
D-->E(5 shell: exec $PYSPARK_PYTHON)
E-->F(6 python: run $PYTHONSTARTUP<pyspark/python/pyspark/shell.py>)
F-->G(7 python: SparkContext._ensure_initialized)
G-->H(8 shell: spark-submit pyspark-shell)
H-->I(9 scala: org.apache.spark.launcher.Main)
I-->J(10 jvm: org.apache.spark.deploy.SparkSubmit pyspark-shell)
J-->K(11 scala: org.apache.spark.deploy.SparkSubmit)
K-->L(12 scala: org.apache.spark.api.python.PythonGatewayServer)
L-->M(13 scala: py4j.GatewayServer.start)
```

1. pyspark是个shell脚本
2. 1会调用另外一个shell命令spark-submit pyspark-shell-main
3. 2又会调用另外一个shell命令spark-class
4. 3里面会执行一个java class，org.apache.spark.launcher.Main重写SparkSubmit参数
5. 3然后会启动一个python进程，这个进程就是最终和用户交互的pyspark
6. 这个python进程启动的时候会先执行环境变量$PYTHONSTARTUP指定的python代码，这个代码就是pyspark/python/pyspark/shell.py，这个环境变量是在1这个shell脚本里设置的，然后我们来看一下shell.py的代码
```
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext

if os.environ.get("SPARK_EXECUTOR_URI"):
    SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])

SparkContext._ensure_initialized()

try:
    spark = SparkSession._create_shell_session()
except Exception:
    import sys
    import traceback
    warnings.warn("Failed to initialize Spark session.")
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

sc = spark.sparkContext
sql = spark.sql
atexit.register(lambda: sc.stop())
```
7. shell.py调用SparkContext._ensure_initialized，接下来的过程和第二种方法一样，选择的class也是org.apache.spark.api.python.PythonGatewayServer，就是起一个py4j.GatewayServer，处理python端发起的请求


# 总结

文章结合代码分析了三种启动PySpark的方法，各有特色，原理是差不多。但是，不同的方法，都可以挖掘一些技巧，实现一些定制的功能、跟自家的产品集成。