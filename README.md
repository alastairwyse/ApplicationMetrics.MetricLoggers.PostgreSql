ApplicationMetrics.MetricLoggers.PostgreSql
---
An implementation of an [ApplicationMetrics](https://github.com/alastairwyse/ApplicationMetrics) [metric logger](https://github.com/alastairwyse/ApplicationMetrics/blob/master/ApplicationMetrics/IMetricLogger.cs) which writes metrics and instrumentation information to a PostgreSQL database.


#### Setup

##### 1) Create/Update the Database and Objects
For new installations, run the [CreateDatabase.sql](https://github.com/alastairwyse/ApplicationMetrics.MetricLoggers.PostgreSql/blob/master/ApplicationMetrics.MetricLoggers.PostgreSql/Resources/CreateDatabase.sql) script against a PostgreSQL instance to create the 'ApplicationMetrics' database and objects to store the metrics.  The 'CREATE DATABASE' statement needs to be run separately, before logging in again under the new database context and running the remainder of the script.  The name of the database can be changed via a find/replace operation on the script (replacing all instances of 'ApplicationMetrics' with a desired database name).  Alternatively, the objects can be created in an existing database.  In any case, the 'Database' component of the connection string passed to the PostgreSqlMetricLogger class should be set to the matching database name.  

##### 2) Setup and Call the PostgreSqlMetricLogger Class

The code below demonstrates the setup and use case (with fake metrics logged) of the PostgreSqlMetricLogger class...

````C#
var connStringBuilder = new NpgsqlConnectionStringBuilder();
connStringBuilder.Host = "127.0.0.1";
connStringBuilder.Database = "ApplicationMetrics";
connStringBuilder.Username = "postgres";
connStringBuilder.Password = "password";

using (var bufferProcessor = new SizeLimitedBufferProcessor(5))
using (var metricLogger = new PostgreSqlMetricLogger("DefaultCategory", connStringBuilder.ToString(), 0, bufferProcessor, IntervalMetricBaseTimeUnit.Millisecond, true))
{
    metricLogger.Start();

    Guid beginId = metricLogger.Begin(new MessageSendTime());
    Thread.Sleep(20);
    metricLogger.Increment(new MessageSent());
    metricLogger.Add(new MessageSize(), 2661);
    metricLogger.End(beginId, new MessageSendTime());

    metricLogger.Stop();
}
````

PostgreSqlMetricLogger accepts the following constructor parameters...

<table>
  <tr>
    <td><b>Parameter Name</b></td>
    <td><b>Description</b></td>
  </tr>
  <tr>
    <td valign="top">category</td>
    <td>
      The category to log the metrics under.  The ability to specify a category allows instances of the same metrics to be logged, but also distinguished from each other... e.g. in the case of a multi-threaded application, the category could be set to reflect an individual thread.
    </td>
  </tr>
  <tr>
    <td valign="top">connectionString</td>
    <td>
      The connection string to connect to PostgreSQL.
    </td>
  </tr>
  <tr>
    <td valign="top">commandTimeout</td>
    <td>
      The time in seconds to wait while trying to execute a command, before terminating the attempt and generating an error. Set to zero for infinity..
    </td>
  </tr>
  <tr>
    <td valign="top">bufferProcessingStrategy</td>
    <td>
      An object implementing <a href="https://github.com/alastairwyse/ApplicationMetrics/blob/master/ApplicationMetrics.MetricLoggers/IBufferProcessingStrategy.cs">IBufferProcessingStrategy</a> which decides when the buffers holding logged metric events should be flushed (and be written to PostgreSQL).
    </td>
  </tr>
  <tr>
    <td valign="top">intervalMetricBaseTimeUnit</td>
    <td>
      The base time unit to use to log interval metrics.
    </td>
  </tr>
  <tr>
    <td valign="top">intervalMetricChecking</td>
    <td>
      Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  This parameter is ignored when the the PostgreSqlMetricLogger operates in <a href="https://github.com/alastairwyse/ApplicationMetrics#interleaved-interval-metrics">'interleaved'</a> mode.
    </td>
  </tr>
  <tr>
    <td valign="top">logger</td>
    <td>
      An optional instance of an <a href="https://github.com/alastairwyse/ApplicationLogging">ApplicationLogging</a> IApplicationLogger instance used to log statistical and performance information (see the 'Logging' section below).
    </td>
  </tr>
</table>

##### 3) Viewing and Querying Logged Metrics
A view is available for each of the 4 different types of metrics (e.g. 'CountMetricInstancesView', 'AmountMetricInstancesView', etc...).  Additionally a view which consolidates all logged metrics ('AllMetricInstancesView') can be queried.  Standard SQL can be used to filter and aggregate the contents of these views.

A sample of the contents of 'AllMetricInstancesView' appears below...

![AllMetricInstancesView contents example](http://alastairwyse.net/applicationmetrics/images/allmetricinstancesview-example.png)

#### Logging

Its possible that ApplicationMetrics and its client application could generate metrics more quickly than a PostgreSQL instance is able to consume them.  In these situations the number of metrics processed, and/or the time taken to process them (depending on the buffer processing strategy used) would continue to increase over time (and eventually lead to out of memory or timeout errors).  If an instance of IApplicationLogger is provided to the constructor, PostgreSqlMetricLogger will create log similar to the following...


```
Processed 550 metric events in 123 milliseconds.
```

...each time a set of buffered metrics are processed, allowing performance to be monitored and the aforementioned situations avoided.

#### Links
The documentation below was written for version 1.* of ApplicationMetrics.  Minor implementation details may have changed in versions 2.0.0 and above, however the basic principles and use cases documented are still valid.  Note also that this documentation demonstrates the older ['non-interleaved'](https://github.com/alastairwyse/ApplicationMetrics#interleaved-interval-metrics) method of logging interval metrics.

Full documentation for the project...<br />
[http://www.alastairwyse.net/methodinvocationremoting/application-metrics.html](http://www.alastairwyse.net/methodinvocationremoting/application-metrics.html)

A detailed sample implementation...<br />
[http://www.alastairwyse.net/methodinvocationremoting/sample-application-5.html](http://www.alastairwyse.net/methodinvocationremoting/sample-application-5.html)

#### Release History

<table>
  <tr>
    <td><b>Version</b></td>
    <td><b>Changes</b></td>
  </tr>
  <tr>
    <td valign="top">1.1.0</td>
    <td>
      Updated for compatibility with ApplicationMetrics version 6.3.0.<br />
    </td>
  </tr>
  <tr>
    <td valign="top">1.0.0</td>
    <td>
      Initial release.
    </td>
  </tr>
</table>