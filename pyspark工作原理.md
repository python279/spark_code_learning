# 前言

Spark是一个开源的通用分布式计算框架，支持海量离线数据处理、实时计算、机器学习、图计算，结合大数据场景，在各个领域都有广泛的应用。Spark支持多种开发语言，包括Python、Java、Scala、R，上手容易。其中，Python因为入门简单、开发效率高（人生苦短，我用Python），广受大数据工程师喜欢，本文主要探讨Pyspark的工作原理。


# 环境准备

因为我的环境是Mac，所以本文一切以Mac环境为前提，不过其它环境过车过都是差不多的。

## Spark环境

首先下载安装Anaconda，[https://www.anaconda.com/distribution/#download-section](https://www.jetbrains.com/idea/download/#section=mac)，选择Python 3.7。

Anaconda安装完之后，开一个终端，执行如下命令安装Pyspark和Openjdk，然后启动Jupyterlab。

- 创建一个虚拟Python环境，名字是test，避免影响Anaconda原始环境
```
% conda create --clone base -n test
% source activate test
```
- 安装Pyspark和Openjdk
```
% conda install pyspark=2.4.4
% conda install openjdk
```
- 安装并启动Jupyterlab
```
% conda install jupyterlab
% jupyter-lab
```

到此会启动一个基于浏览器的开发环境，可用于编写、调试Python代码。

## 阅读Spark代码环境

Spark本身是用Scala、Java、Python开发的，建议安装IntelliJ IDEA ([https://www.jetbrains.com/idea/download/#section=mac](https://www.jetbrains.com/idea/download/#section=mac))。

安装完IDEA，通过下面的命令下载Spark-2.4.4的代码。
```
% git clone https://github.com/apache/spark.git
% cd spark
% git checkout v2.4.4
```

代码下载完之后，打开IEDA，选择New->Project from existing sources，新建一个项目，IDEA会扫描整个项目、下载依赖，完成之后就可以阅读代码了。


# 深入Pyspark

## Pyspark用法

在学习Pyspark的工作原理之前，我们先看看Pyspark是怎么用的，先看一段代码。代码很简单，首先创建spark session，然后从csv文件创建dataframe，最后通过rdd的map算子转换数据形式。中间利用了自定义函数test来转换输入数据，test函数的输入数据是一行数据。
```
from pyspark.sql import SparkSession
from pyspark.sql import Row

# 创建spark session
spark = SparkSession \
    .builder \
    .appName("pyspark demo") \
    .getOrCreate()

# 从csv文件创建dataframe
df = spark.read.csv("stock.csv", header=True)

# 自定义分布式函数，将输入行转成另外一种形式
def test(r):
    return repr(r)

# dataframe转成RDD，通过map转换数据形式，最后获取10条数据
df.rdd.map(lambda r: test(r)).take(10)
```

通过在Jupyterlab里面启动spark session之后，我们来看一下相关的进程父子关系。05920是Jupyterlab进程，我启动一个Python kernel，进程05964。然后启动spark session，这是一个Java进程，ID是06450。同时Spark java进程启动了一个Python守护进程，这个进程是处理PythonRDD数据的。因为我起的Spark是local模式，所以只有一个Spark进程和一个Python进程。如果是yarn模式，每一个executor都会启动一个Python进程，PythonRDD在Python守护进程里处理然后返回结果给Spark Task线程。
```
 | |   \-+= 05920 haiqiangli /Users/haiqiangli/anaconda3/envs/ml/bin/python3.7 /Users/haiqiangli/anaconda3/envs/ml/bin/jupyter-lab
 | |     \-+= 05964 haiqiangli /Users/haiqiangli/anaconda3/envs/ml/bin/python -m ipykernel_launcher -f /Users/haiqiangli/Library/Jupyter/runtime/kernel-62a08e01-a4c7-4fe6-b92f-621e9967197e.json
 | |       \-+- 06450 haiqiangli /Users/haiqiangli/anaconda3/envs/ml/jre/bin/java -cp /Users/haiqiangli/anaconda3/envs/ml/lib/python3.7/site-packages/pyspark/conf:/Users/haiqiangli/anaconda3/envs/ml/lib/python3.7/site-packages/pyspark/jars/* -Xmx1g org.
 | |         \--= 06750 haiqiangli python -m pyspark.daemon
```

## PythonRDD实现

我们从这段代码开始分析，先看df.rdd，代码在pyspark/sql/dataframe.py。
```
df.rdd.map(lambda r: test(r)).take(10)
```

jrdd是通过py4j调用Java代码将Spark driver内部当前这个dataframe转成Python rdd，类RDD是Python rdd的封装，我们看一下Python rdd的定义，代码在pyspark/rdd.py。
```
@property
@since(1.3)
def rdd(self):
    """Returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    """
    if self._lazy_rdd is None:
        jrdd = self._jdf.javaToPython()
        self._lazy_rdd = RDD(jrdd, self.sql_ctx._sc, BatchedSerializer(PickleSerializer()))
    return self._lazy_rdd
```
```
class RDD(object):

    """
    A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
    Represents an immutable, partitioned collection of elements that can be
    operated on in parallel.
    """

    def __init__(self, jrdd, ctx, jrdd_deserializer=AutoBatchedSerializer(PickleSerializer())):
        self._jrdd = jrdd
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = ctx
        self._jrdd_deserializer = jrdd_deserializer
        self._id = jrdd.id()
        self.partitioner = None
    
    ...
```

现在来看一下rdd.map的实现，代码如下。map接口先定义一个闭包函数func（引用lambda r: test(r)），然后再调用mapPartitionsWithIndex。mapPartitionsWithIndex只返回了新的对象PipelinedRDD，也就是说map会返回一个新的RDD对象（PipelinedRDD），我们来看一下PipelinedRDD的定义，self.func就是map里面定义的闭包函数func，这个很重要，后面会再次用到。
```
def map(self, f, preservesPartitioning=False):
    def func(_, iterator):
        return map(fail_on_stopiteration(f), iterator)
    return self.mapPartitionsWithIndex(func, preservesPartitioning)

def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
    return PipelinedRDD(self, f, preservesPartitioning)

class PipelinedRDD(RDD):

    def __init__(self, prev, func, preservesPartitioning=False, isFromBarrier=False):
        if not isinstance(prev, PipelinedRDD) or not prev._is_pipelinable():
            # This transformation is the first in its stage:
            self.func = func
            self.preservesPartitioning = preservesPartitioning
            self._prev_jrdd = prev._jrdd
            self._prev_jrdd_deserializer = prev._jrdd_deserializer
        else:
            prev_func = prev.func

            def pipeline_func(split, iterator):
                return func(split, prev_func(split, iterator))
            self.func = pipeline_func
            self.preservesPartitioning = \
                prev.preservesPartitioning and preservesPartitioning
            self._prev_jrdd = prev._prev_jrdd  # maintain the pipeline
            self._prev_jrdd_deserializer = prev._prev_jrdd_deserializer
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = prev.ctx
        self.prev = prev
        self._jrdd_val = None
        self._id = None
        self._jrdd_deserializer = self.ctx.serializer
        self._bypass_serializer = False
        self.partitioner = prev.partitioner if self.preservesPartitioning else None
        self.is_barrier = prev._is_barrier() or isFromBarrier
```

现在我们看一下df.rdd.map(lambda r: test(r)).take(10)里面的take，提醒一下map操作只是一个transform，不会触发真正的计算任务，只有action会，这里的take就是一个action。take会触发当前RDD的transform，包括它的父RDD，最终返回num条数据。我们看一下take函数，有点长，最关键的是self.context.runJob。
```
    def take(self, num):
        items = []
        totalParts = self.getNumPartitions()
        partsScanned = 0

        while len(items) < num and partsScanned < totalParts:
            # The number of partitions to try in this iteration.
            # It is ok for this number to be greater than totalParts because
            # we actually cap it at totalParts in runJob.
            numPartsToTry = 1
            if partsScanned > 0:
                # If we didn't find any rows after the previous iteration,
                # quadruple and retry.  Otherwise, interpolate the number of
                # partitions we need to try, but overestimate it by 50%.
                # We also cap the estimation in the end.
                if len(items) == 0:
                    numPartsToTry = partsScanned * 4
                else:
                    # the first parameter of max is >=1 whenever partsScanned >= 2
                    numPartsToTry = int(1.5 * num * partsScanned / len(items)) - partsScanned
                    numPartsToTry = min(max(numPartsToTry, 1), partsScanned * 4)

            left = num - len(items)

            def takeUpToNumLeft(iterator):
                iterator = iter(iterator)
                taken = 0
                while taken < left:
                    try:
                        yield next(iterator)
                    except StopIteration:
                        return
                    taken += 1

            p = range(partsScanned, min(partsScanned + numPartsToTry, totalParts))
            res = self.context.runJob(self, takeUpToNumLeft, p)

            items += res
            partsScanned += numPartsToTry

        return items[:num]
```

接着我们看self.context.runJob代码（pyspark/context.py）,主要看self._jvm.PythonRDD.runJob(self._jsc.sc(), mappedRDD._jrdd, partitions)这行，其中_jrdd是在PipelinedRDD里面定义，看代码。
```
def runJob(self, rdd, partitionFunc, partitions=None, allowLocal=False):
    if partitions is None:
        partitions = range(rdd._jrdd.partitions().size())

    # Implementation note: This is implemented as a mapPartitions followed
    # by runJob() in order to avoid having to pass a Python lambda into
    # SparkContext#runJob.
    mappedRDD = rdd.mapPartitions(partitionFunc)
    sock_info = self._jvm.PythonRDD.runJob(self._jsc.sc(), mappedRDD._jrdd, partitions)
    return list(_load_from_socket(sock_info, mappedRDD._jrdd_deserializer))
```

_jrdd代码是Spark支持Python API的关键，_wrap_function这里是序列化上面定义的闭包函数func以及它的所有依赖，我们知道这个函数是被分布式算子map调用的函数，这个函数会在executor上执行，确切的说是executor上启动的Python守护进程里执行。因此这里Python必须序列化并打包这个func函数和它的执行环境，随后会在executor的Python进程里加载，这样就完成了分布式函数的自动广播操作。有点烧脑，关于Python函数序列化的内容我们放到单独一篇文章里再讲。
```
@property
def _jrdd(self):
    if self._jrdd_val:
        return self._jrdd_val
    if self._bypass_serializer:
        self._jrdd_deserializer = NoOpSerializer()

    if self.ctx.profiler_collector:
        profiler = self.ctx.profiler_collector.new_profiler(self.ctx)
    else:
        profiler = None

    wrapped_func = _wrap_function(self.ctx, self.func, self._prev_jrdd_deserializer,
                                  self._jrdd_deserializer, profiler)
    python_rdd = self.ctx._jvm.PythonRDD(self._prev_jrdd.rdd(), wrapped_func,
                                         self.preservesPartitioning, self.is_barrier)
    self._jrdd_val = python_rdd.asJavaRDD()

    if profiler:
        self._id = self._jrdd_val.id()
        self.ctx.profiler_collector.add_profiler(self._id, profiler)
    return self._jrdd_val
```

继续看_wrap_function，_prepare_for_python_RDD就是序列化Python函数的过程，细节下一篇再讲。再看一下sc._jvm.PythonFunction，这个是scala写的类，代码在core/src/main/scala/org/apache/spark/api/pythn/PythonRDD.scala，这个类封装了一些Python必需的数据和环境。
```
def _wrap_function(sc, func, deserializer, serializer, profiler=None):
    assert deserializer, "deserializer should not be empty"
    assert serializer, "serializer should not be empty"
    command = (func, profiler, deserializer, serializer)
    pickled_command, broadcast_vars, env, includes = _prepare_for_python_RDD(sc, command)
    return sc._jvm.PythonFunction(bytearray(pickled_command), env, includes, sc.pythonExec,
                                  sc.pythonVer, broadcast_vars, sc._javaAccumulator)
```
```
/**
 * A wrapper for a Python function, contains all necessary context to run the function in Python
 * runner.
 */
private[spark] case class PythonFunction(
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    pythonVer: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: PythonAccumulatorV2)
```

继续看_jrdd代码，sock_info = self._jvm.PythonRDD.runJob(self._jsc.sc(), mappedRDD._jrdd, partitions)这里创建了一个新对象，来看一下runJob的定义。
```
  def runJob(
      sc: SparkContext,
      rdd: JavaRDD[Array[Byte]],
      partitions: JArrayList[Int]): Array[Any] = {
    type ByteArray = Array[Byte]
    type UnrolledPartition = Array[ByteArray]
    val allPartitions: Array[UnrolledPartition] =
      sc.runJob(rdd, (x: Iterator[ByteArray]) => x.toArray, partitions.asScala)
    val flattenedPartition: UnrolledPartition = Array.concat(allPartitions: _*)
    serveIterator(flattenedPartition.iterator,
      s"serve RDD ${rdd.id} with partitions ${partitions.asScala.mkString(",")}")
  }
```

PythonRDD.runJob调用了SparkContext.runJob，再来看看这个runJob的定义。看到我们熟悉的dagScheduler，它是Spark的核心，dag将RDD依赖划分到不同的Stage，构建这些Stage的父子关系，最后将Stage按照Partition切分成多个Task。这些细节不是本文的重点，后面再讲。
```
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }
```

最后我们看一下，在什么地方会用到上面定义的Python函数func。还记得之前给的Pyspark的进程父子关系，其中06750 haiqiangli python -m pyspark.daemon这个进程是Spark java的子进程，我们来看一下它的实现（pysark/daemon.py）。一开始创建sock，随机分配一个监听端口。然后通过write_int(listen_port, stdout_bin)写标准输出，把自己的监听端口号告诉父进程。接着通过epoll的方式监听连接，一旦有连接就会创建一个子进程来处理这个连接的请求，为了提高性能。
```
def manager():
    # Create a new process group to corral our children
    os.setpgid(0, 0)

    # Create a listening socket on the AF_INET loopback interface
    listen_sock = socket.socket(AF_INET, SOCK_STREAM)
    listen_sock.bind(('127.0.0.1', 0))
    listen_sock.listen(max(1024, SOMAXCONN))
    listen_host, listen_port = listen_sock.getsockname()

    # re-open stdin/stdout in 'wb' mode
    stdin_bin = os.fdopen(sys.stdin.fileno(), 'rb', 4)
    stdout_bin = os.fdopen(sys.stdout.fileno(), 'wb', 4)
    write_int(listen_port, stdout_bin)
    stdout_bin.flush()

    reuse = os.environ.get("SPARK_REUSE_WORKER")

    # Initialization complete
    try:
        while True:
            try:
                ready_fds = select.select([0, listen_sock], [], [], 1)[0]
            except select.error as ex:
                if ex[0] == EINTR:
                    continue
                else:
                    raise

            if 0 in ready_fds:
                try:
                    worker_pid = read_int(stdin_bin)
                except EOFError:
                    # Spark told us to exit by closing stdin
                    shutdown(0)
                try:
                    os.kill(worker_pid, signal.SIGKILL)
                except OSError:
                    pass  # process already died

            if listen_sock in ready_fds:
                try:
                    sock, _ = listen_sock.accept()
                except OSError as e:
                    if e.errno == EINTR:
                        continue
                    raise

                # Launch a worker process
                try:
                    pid = os.fork()
                except OSError as e:
                    if e.errno in (EAGAIN, EINTR):
                        time.sleep(1)
                        pid = os.fork()  # error here will shutdown daemon
                    else:
                        outfile = sock.makefile(mode='wb')
                        write_int(e.errno, outfile)  # Signal that the fork failed
                        outfile.flush()
                        outfile.close()
                        sock.close()
                        continue

                if pid == 0:
                    # in child process
                    listen_sock.close()
                    try:
                        # Acknowledge that the fork was successful
                        outfile = sock.makefile(mode="wb")
                        write_int(os.getpid(), outfile)
                        outfile.flush()
                        outfile.close()
                        authenticated = False
                        while True:
                            code = worker(sock, authenticated)
                            if code == 0:
                                authenticated = True
                            if not reuse or code:
                                # wait for closing
                                try:
                                    while sock.recv(1024):
                                        pass
                                except Exception:
                                    pass
                                break
                            gc.collect()
                    except:
                        traceback.print_exc()
                        os._exit(1)
                    else:
                        os._exit(0)
                else:
                    sock.close()

    finally:
        shutdown(1)
```

子进程创建两个文件句柄，infile和outfile，都是在那个sock基础上，然后调用worker_main(infile, outfile)。
```
def worker(sock, authenticated):
    """
    Called by a worker process after the fork().
    """
    signal.signal(SIGHUP, SIG_DFL)
    signal.signal(SIGCHLD, SIG_DFL)
    signal.signal(SIGTERM, SIG_DFL)
    # restore the handler for SIGINT,
    # it's useful for debugging (show the stacktrace before exit)
    signal.signal(SIGINT, signal.default_int_handler)

    # Read the socket using fdopen instead of socket.makefile() because the latter
    # seems to be very slow; note that we need to dup() the file descriptor because
    # otherwise writes also cause a seek that makes us miss data on the read side.
    infile = os.fdopen(os.dup(sock.fileno()), "rb", 65536)
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", 65536)

    if not authenticated:
        client_secret = UTF8Deserializer().loads(infile)
        if os.environ["PYTHON_WORKER_FACTORY_SECRET"] == client_secret:
            write_with_length("ok".encode("utf-8"), outfile)
            outfile.flush()
        else:
            write_with_length("err".encode("utf-8"), outfile)
            outfile.flush()
            sock.close()
            return 1

    exit_code = 0
    try:
        worker_main(infile, outfile)
    except SystemExit as exc:
        exit_code = compute_real_exit_code(exc.code)
    finally:
        try:
            outfile.flush()
        except Exception:
            pass
    return exit_code
```

worker_main代码比较长，我们只看核心的部分，func, profiler, deserializer, serializer = read_command(pickleSer, infile)这行是不是很熟悉，反序列化出函数func，然后在process里面处理sock输入的数据，再写回sock。
```
        if eval_type == PythonEvalType.NON_UDF:
            func, profiler, deserializer, serializer = read_command(pickleSer, infile)
        else:
            func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)

        init_time = time.time()

        def process():
            iterator = deserializer.load_stream(infile)
            serializer.dump_stream(func(split_index, iterator), outfile)
```

到此已经基本讲清楚PythonRDD的实现，正是因为Spark contributer的贡献，我们才能非常方便地通过Python开发Spark程序，让更多的数据分析师、机器学习工程师受益，在此对开源contributer们致以最崇高的敬意！