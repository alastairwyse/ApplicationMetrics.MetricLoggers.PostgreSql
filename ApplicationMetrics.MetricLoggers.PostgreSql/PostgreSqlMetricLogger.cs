/*
* Copyright 2024 Alastair Wyse (https://github.com/alastairwyse/ApplicationMetrics.MetricLoggers.PostgreSql/)
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ApplicationMetrics.MetricLoggers;
using ApplicationLogging;
using StandardAbstraction;
using Npgsql;
using NpgsqlTypes;

namespace ApplicationMetrics.MetricLoggers.PostgreSql
{
    /// <summary>
    /// Writes metric events to a PostgreSQL database.
    /// </summary>
    public class PostgreSqlMetricLogger : MetricLoggerBuffer, IDisposable
    {
        // In the base MetricLoggerBuffer class, methods Process*MetricEvents() are called synchronously in sequence.
        // However since in this implementation the different metric types are written to different tables in PostgreSQL, we can improve performance by executing the work
        //   of each of these methods in parallel in worker threads.
        // Hence Tasks and thread signalling classes are used in this class to execute the work of the 4 methods in parallel, but not return control from the final
        //   ProcessIntervalMetricEvents() method until all parallel work is completed.

        #pragma warning disable 1591

        protected const String insertCountMetricsStoredProcedureName = "InsertCountMetrics";
        protected const String insertAmountMetricsStoredProcedureName = "InsertAmountMetrics";
        protected const String insertStatusMetricsStoredProcedureName = "InsertStatusMetrics";
        protected const String insertIntervalMetricsStoredProcedureName = "InsertIntervalMetrics";
        protected const String namePropertyName = "Name";
        protected const String descriptionPropertyName = "Description";
        protected const String timePropertyName = "Time";
        protected const String amountPropertyName = "Amount";
        protected const String valuePropertyName = "Value";
        protected const String durationPropertyName = "Duration";

        #pragma warning restore 1591

        /// <summary>DateTime format string which can be interpreted by the <see href="https://www.postgresql.org/docs/8.1/functions-formatting.html">PostgreSQL to_timestamp() function</see>.</summary>
        protected const String postgreSQLTimestampFormat = "yyyy-MM-dd HH:mm:ss.ffffff";

        /// <summary>The category to log all metrics under.</summary>
        protected String category;
        /// <summary>The string to use to connect to the PostgreSQL database.</summary>
        protected String connectionString;
        /// <summary>The time in seconds to wait while trying to execute a command, before terminating the attempt and generating an error. Set to zero for infinity.</summary>
        protected Int32 commandTimeout;
        /// <summary>The datasource to use to create connections to PostgreSQL.</summary>
        protected NpgsqlDataSource dataSource;
        /// <summary>Signal that is set by the designated first Process*MetricEvents() method is called.  Allows that method to properly initialize object state before the other methods call PostgreSQL.</summary>
        protected ManualResetEvent parallelProcessStartSignal;
        /// <summary>Signal that is waited on by the designated last Process*MetricEvents() method before returning.  Allows that method to ensure that all the other methods have been called and completed before control is returned to the base class DequeueAndProcessMetricEvents() method.</summary>
        protected CountdownEvent parallelProcessCompletedSignal;
        /// <summary>Holds any exceptions which are thrown on worker threads.</summary>
        protected ConcurrentQueue<Exception> workerThreadExceptions;
        /// <summary>Whether an exception occurred on one of the threads sending events to the the PostgreSQL database.</summary>
        protected volatile Int32 processedEventCount;
        /// <summary>Holds the time the calls to the Process*MetricEvents() methods started.</summary>
        protected System.DateTime processingStartTime;
        /// <summary>The logger to use for performance statistics.</summary>
        protected IApplicationLogger logger;
        /// <summary>Wraps calls to execute stored procedures so that they can be mocked in unit tests.</summary>
        protected IStoredProcedureExecutionWrapper storedProcedureExecutor;

        #pragma warning disable 8618

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.PostgreSQL.PostgreSqlMetricLogger class.
        /// </summary>
        /// <param name="category">The category to log all metrics under.</param>
        /// <param name="connectionString">The string to use to connect to the PostgreSQL database.</param>
        /// <param name="commandTimeout">The time in seconds to wait while trying to execute a command, before terminating the attempt and generating an error. Set to zero for infinity.</param>
        /// <param name="bufferProcessingStrategy">Object which implements a processing strategy for the buffers (queues).</param>
        /// <param name="intervalMetricBaseTimeUnit">The base time unit to use to log interval metrics.</param>
        /// <param name="intervalMetricChecking">Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode.</param>
        /// <remarks>The class uses a <see cref="System.Diagnostics.Stopwatch"/> to calculate and log interval metrics.  Since the smallest unit of time supported by Stopwatch is a tick (100 nanoseconds), the smallest level of granularity supported when constructor parameter 'intervalMetricBaseTimeUnit' is set to <see cref="IntervalMetricBaseTimeUnit.Nanosecond"/> is 100 nanoseconds.</remarks>
        public PostgreSqlMetricLogger(String category, String connectionString, Int32 commandTimeout, IBufferProcessingStrategy bufferProcessingStrategy, IntervalMetricBaseTimeUnit intervalMetricBaseTimeUnit, Boolean intervalMetricChecking)
            : base(bufferProcessingStrategy, intervalMetricBaseTimeUnit, intervalMetricChecking)
        {
            ValidateAndInitialiseConstructorParameters(category, connectionString, commandTimeout);
        }

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.PostgreSQL.PostgreSqlMetricLogger class.
        /// </summary>
        /// <param name="category">The category to log all metrics under.</param>
        /// <param name="connectionString">The string to use to connect to the PostgreSQL database.</param>
        /// <param name="commandTimeout">The time in seconds to wait while trying to execute a command, before terminating the attempt and generating an error. Set to zero for infinity.</param>
        /// <param name="bufferProcessingStrategy">Object which implements a processing strategy for the buffers (queues).</param>
        /// <param name="intervalMetricBaseTimeUnit">The base time unit to use to log interval metrics.</param>
        /// <param name="intervalMetricChecking">Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode.</param>
        /// <param name="logger">The logger to use for performance statistics.</param>
        /// <remarks>The class uses a <see cref="System.Diagnostics.Stopwatch"/> to calculate and log interval metrics.  Since the smallest unit of time supported by Stopwatch is a tick (100 nanoseconds), the smallest level of granularity supported when constructor parameter 'intervalMetricBaseTimeUnit' is set to <see cref="IntervalMetricBaseTimeUnit.Nanosecond"/> is 100 nanoseconds.</remarks>
        public PostgreSqlMetricLogger
        (
            String category, 
            String connectionString, 
            Int32 commandTimeout, 
            IBufferProcessingStrategy bufferProcessingStrategy, 
            IntervalMetricBaseTimeUnit intervalMetricBaseTimeUnit, 
            Boolean intervalMetricChecking, 
            IApplicationLogger logger
        )
            : base(bufferProcessingStrategy, intervalMetricBaseTimeUnit, intervalMetricChecking)
        {
            ValidateAndInitialiseConstructorParameters(category, connectionString, commandTimeout);
            this.logger = logger;
        }

        /// <summary>
        /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.PostgreSQL.PostgreSqlMetricLogger class.
        /// </summary>
        /// <param name="category">The category to log all metrics under.</param>
        /// <param name="connectionString">The string to use to connect to the PostgreSQL database.</param>
        /// <param name="commandTimeout">The time in seconds to wait while trying to execute a command, before terminating the attempt and generating an error. Set to zero for infinity.</param>
        /// <param name="bufferProcessingStrategy">Object which implements a processing strategy for the buffers (queues).</param>
        /// <param name="intervalMetricBaseTimeUnit">The base time unit to use to log interval metrics.</param>
        /// <param name="intervalMetricChecking">Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode.</param>
        /// <param name="logger">The logger to use for performance statistics.</param>
        /// <param name="dateTime">A test (mock) <see cref="System.DateTime"/> object.</param>
        /// <param name="stopWatch">A test (mock) <see cref="System.Diagnostics.Stopwatch"/> object.</param>
        /// <param name="guidProvider">A test (mock) <see cref="IGuidProvider"/> object.</param>
        /// <param name="storedProcedureExecutor">A test (mock) <see cref="IStoredProcedureExecutionWrapper"/> object.</param>
        /// <remarks>This constructor is included to facilitate unit testing.</remarks>
        public PostgreSqlMetricLogger
        (
            String category,
            String connectionString,
            Int32 commandTimeout,
            IBufferProcessingStrategy bufferProcessingStrategy,
            IntervalMetricBaseTimeUnit intervalMetricBaseTimeUnit,
            Boolean intervalMetricChecking,
            IApplicationLogger logger,
            IDateTime dateTime,
            IStopwatch stopWatch,
            IGuidProvider guidProvider,
            IStoredProcedureExecutionWrapper storedProcedureExecutor
        )
            : base(bufferProcessingStrategy, intervalMetricBaseTimeUnit, intervalMetricChecking, dateTime, stopWatch, guidProvider)
        {
            ValidateAndInitialiseConstructorParameters(category, connectionString, commandTimeout);
            this.storedProcedureExecutor = storedProcedureExecutor;
            this.logger = logger;
        }

        #pragma warning restore 8618

        #region Base Class Abstract Method Implementations

        /// <inheritdoc/>
        protected override void ProcessCountMetricEvents(Queue<CountMetricEventInstance> countMetricEvents)
        {
            if (workerThreadExceptions.Count > 0)
            {
                throw new AggregateException("One or more exceptions occurred on worker threads whilst writing metrics to PostgreSQL.", workerThreadExceptions);
            }
            parallelProcessCompletedSignal.Reset();
            processedEventCount = 0;
            processingStartTime = GetStopWatchUtcNow();
            // Allow the other parallel calls to PostgreSQL on worker threads to start
            parallelProcessStartSignal.Set();

            Task.Run(() =>
            {
                var encounteredMetricTypes = new HashSet<Type>();
                using (var memoryStream = new MemoryStream())
                using (var writer = new Utf8JsonWriter(memoryStream))
                {
                    writer.WriteStartArray();
                    foreach (CountMetricEventInstance currentCountMetricEvent in countMetricEvents)
                    {
                        Boolean writeDescription = true;
                        if (encounteredMetricTypes.Contains(currentCountMetricEvent.MetricType))
                        {
                            writeDescription = false;
                        }
                        else
                        {
                            encounteredMetricTypes.Add(currentCountMetricEvent.MetricType);
                        }
                        writer.WriteStartObject();
                        WriteEventInstancePropertiesToJson(currentCountMetricEvent, writer, writeDescription);
                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();
                    writer.Flush();
                    memoryStream.Position = 0;
                    using (var metricEventsJson = JsonDocument.Parse(memoryStream))
                    {
                        var parameters = new List<NpgsqlParameter>()
                        {
                            CreateNpgsqlParameterWithValue(NpgsqlDbType.Varchar, category), 
                            CreateNpgsqlParameterWithValue(NpgsqlDbType.Json, metricEventsJson)
                        };
                        try
                        {
                            storedProcedureExecutor.Execute(insertCountMetricsStoredProcedureName, parameters);
                            Interlocked.Add(ref processedEventCount, countMetricEvents.Count);
                        }
                        catch (Exception e)
                        {
                            var outerException = new Exception("An error occurred writing count metrics to PostgreSQL.", e);
                            workerThreadExceptions.Enqueue(outerException);
                            throw e;
                        }
                        finally
                        {
                            parallelProcessCompletedSignal.Signal();
                        }
                    }
                }
            });
        }

        /// <inheritdoc/>
        protected override void ProcessAmountMetricEvents(Queue<AmountMetricEventInstance> amountMetricEvents)
        {
            Task.Run(() =>
            {
                // Wait for the 'first' Process*MetricEvents() to reset the complete signal
                parallelProcessStartSignal.WaitOne();

                var encounteredMetricTypes = new HashSet<Type>();
                using (var memoryStream = new MemoryStream())
                using (var writer = new Utf8JsonWriter(memoryStream))
                {
                    writer.WriteStartArray();
                    foreach (AmountMetricEventInstance currentAmountMetricEvent in amountMetricEvents)
                    {
                        Boolean writeDescription = true;
                        if (encounteredMetricTypes.Contains(currentAmountMetricEvent.MetricType))
                        {
                            writeDescription = false;
                        }
                        else
                        {
                            encounteredMetricTypes.Add(currentAmountMetricEvent.MetricType);
                        }
                        writer.WriteStartObject();
                        WriteEventInstancePropertiesToJson(currentAmountMetricEvent, writer, writeDescription);
                        writer.WriteString(amountPropertyName, currentAmountMetricEvent.Amount.ToString());
                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();
                    writer.Flush();
                    memoryStream.Position = 0;
                    using (var metricEventsJson = JsonDocument.Parse(memoryStream))
                    {
                        var parameters = new List<NpgsqlParameter>()
                        {
                            CreateNpgsqlParameterWithValue(NpgsqlDbType.Varchar, category),
                            CreateNpgsqlParameterWithValue(NpgsqlDbType.Json, metricEventsJson)
                        };
                        try
                        {
                            storedProcedureExecutor.Execute(insertAmountMetricsStoredProcedureName, parameters);
                            Interlocked.Add(ref processedEventCount, amountMetricEvents.Count);
                        }
                        catch (Exception e)
                        {
                            var outerException = new Exception("An error occurred writing amount metrics to PostgreSQL.", e);
                            workerThreadExceptions.Enqueue(outerException);
                            throw e;
                        }
                        finally
                        {
                            parallelProcessCompletedSignal.Signal();
                        }
                    }
                }
            });
        }

        /// <inheritdoc/>
        protected override void ProcessStatusMetricEvents(Queue<StatusMetricEventInstance> statusMetricEvents)
        {
            Task.Run(() =>
            {
                // Wait for the 'first' Process*MetricEvents() to reset the complete signal
                parallelProcessStartSignal.WaitOne();

                var encounteredMetricTypes = new HashSet<Type>();
                using (var memoryStream = new MemoryStream())
                using (var writer = new Utf8JsonWriter(memoryStream))
                {
                    writer.WriteStartArray();
                    foreach (StatusMetricEventInstance currentStatusMetricEvent in statusMetricEvents)
                    {
                        Boolean writeDescription = true;
                        if (encounteredMetricTypes.Contains(currentStatusMetricEvent.MetricType))
                        {
                            writeDescription = false;
                        }
                        else
                        {
                            encounteredMetricTypes.Add(currentStatusMetricEvent.MetricType);
                        }
                        writer.WriteStartObject();
                        WriteEventInstancePropertiesToJson(currentStatusMetricEvent, writer, writeDescription);
                        writer.WriteString(valuePropertyName, currentStatusMetricEvent.Value.ToString());
                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();
                    writer.Flush();
                    memoryStream.Position = 0;
                    using (var metricEventsJson = JsonDocument.Parse(memoryStream))
                    {
                        var parameters = new List<NpgsqlParameter>()
                        {
                            CreateNpgsqlParameterWithValue(NpgsqlDbType.Varchar, category),
                            CreateNpgsqlParameterWithValue(NpgsqlDbType.Json, metricEventsJson)
                        };
                        try
                        {
                            storedProcedureExecutor.Execute(insertStatusMetricsStoredProcedureName, parameters);
                            Interlocked.Add(ref processedEventCount, statusMetricEvents.Count);
                        }
                        catch (Exception e)
                        {
                            var outerException = new Exception("An error occurred writing status metrics to PostgreSQL.", e);
                            workerThreadExceptions.Enqueue(outerException);
                            throw e;
                        }
                        finally
                        {
                            parallelProcessCompletedSignal.Signal();
                        }
                    }
                }
            });
        }

        /// <inheritdoc/>
        protected override void ProcessIntervalMetricEvents(Queue<Tuple<IntervalMetricEventInstance, Int64>> intervalMetricEventsAndDurations)
        {
            // Wait for the 'first' Process*MetricEvents() to reset the complete signal
            parallelProcessStartSignal.WaitOne();

            var encounteredMetricTypes = new HashSet<Type>();
            using (var memoryStream = new MemoryStream())
            using (var writer = new Utf8JsonWriter(memoryStream))
            {
                writer.WriteStartArray();
                foreach (Tuple<IntervalMetricEventInstance, Int64> currentIntervalMetricEvent in intervalMetricEventsAndDurations)
                {
                    Boolean writeDescription = true;
                    if (encounteredMetricTypes.Contains(currentIntervalMetricEvent.Item1.MetricType))
                    {
                        writeDescription = false;
                    }
                    else
                    {
                        encounteredMetricTypes.Add(currentIntervalMetricEvent.Item1.MetricType);
                    }
                    writer.WriteStartObject();
                    WriteEventInstancePropertiesToJson(currentIntervalMetricEvent.Item1, writer, writeDescription);
                    writer.WriteString(durationPropertyName, currentIntervalMetricEvent.Item2.ToString());
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
                writer.Flush();
                memoryStream.Position = 0;
                using (var metricEventsJson = JsonDocument.Parse(memoryStream))
                {
                    var parameters = new List<NpgsqlParameter>()
                    {
                        CreateNpgsqlParameterWithValue(NpgsqlDbType.Varchar, category),
                        CreateNpgsqlParameterWithValue(NpgsqlDbType.Json, metricEventsJson)
                    };
                    try
                    {
                        storedProcedureExecutor.Execute(insertIntervalMetricsStoredProcedureName, parameters);
                        Interlocked.Add(ref processedEventCount, intervalMetricEventsAndDurations.Count);
                    }
                    catch (Exception e)
                    {
                        var outerException = new Exception("An error occurred writing interval metrics to PostgreSQL.", e);
                        workerThreadExceptions.Enqueue(outerException);
                        // Since this 'last' Process*MetricEvents() method runs on the main thread, don't throw the exception here, as it would hide/mask any exceptions which occurred on the worker threads running the other Process*MetricEvents() methods
                    }
                    finally
                    {
                        parallelProcessCompletedSignal.Wait();
                        Int32 processingTime = Convert.ToInt32(Math.Round((base.GetStopWatchUtcNow() - processingStartTime).TotalMilliseconds));
                        logger.Log(this, LogLevel.Information, $"Processed {processedEventCount} metric events in {processingTime} milliseconds.");
                        parallelProcessStartSignal.Reset();
                    }
                }
            }
        }

        #endregion

        #region Private/Protected Methods

        /// <summary>
        /// Common method to validate and set/initialise parameters passed to the constructor.
        /// </summary>
        /// <param name="category">The category to log all metrics under.</param>
        /// <param name="connectionString">The string to use to connect to the PostgreSQL database.</param>
        /// <param name="commandTimeout">The time in seconds to wait while trying to execute a command, before terminating the attempt and generating an error. Set to zero for infinity.</param>
        protected void ValidateAndInitialiseConstructorParameters(String category, String connectionString, Int32 commandTimeout)
        {
            if (String.IsNullOrWhiteSpace(category) == true)
                throw new ArgumentException($"Parameter '{nameof(category)}' must contain a value.", nameof(category));
            if (String.IsNullOrWhiteSpace(connectionString) == true)
                throw new ArgumentException($"Parameter '{nameof(connectionString)}' must contain a value.", nameof(connectionString));
            if (commandTimeout < 0)
                throw new ArgumentOutOfRangeException(nameof(commandTimeout), $"Parameter '{nameof(commandTimeout)}' with value {commandTimeout} cannot be less than 0.");

            this.category = category;
            this.connectionString = connectionString;
            this.commandTimeout = commandTimeout;
            NpgsqlDataSourceBuilder dataSourceBuilder;
            try
            {
                dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionString);
                dataSource = dataSourceBuilder.Build();
            }
            catch (Exception e)
            {
                throw new Exception($"Failed to create {typeof(NpgsqlDataSource).Name} from connection string '{connectionString}'.", e);
            }
            parallelProcessStartSignal = new ManualResetEvent(false);
            parallelProcessCompletedSignal = new CountdownEvent(3);
            workerThreadExceptions = new ConcurrentQueue<Exception>();
            storedProcedureExecutor = new StoredProcedureExecutionWrapper((String procedureName, IList<NpgsqlParameter> parameters) => { ExecuteStoredProcedure(procedureName, parameters); });
            logger = new NullLogger();
        }

        /// <summary>
        /// Writes base/common properties of a metric event instance to the specified <see cref="Utf8JsonWriter"/>.
        /// </summary>
        /// <typeparam name="T">The type of metric (deriving from <see cref="MetricBase"/>) to write the properties of.</typeparam>
        /// <param name="metricEventInstance">The metric event instance to write.</param>
        /// <param name="writer">The <see cref="Utf8JsonWriter"/> to write to.</param>
        /// <param name="writeDescription">Whether to write the description property (true) or an empty string (false).</param>
        protected void WriteEventInstancePropertiesToJson<T>(MetricEventInstance<T> metricEventInstance, Utf8JsonWriter writer, Boolean writeDescription)
            where T : MetricBase
        {
            writer.WriteString(namePropertyName, metricEventInstance.Metric.Name);
            if (writeDescription == true)
            {
                writer.WriteString(descriptionPropertyName, metricEventInstance.Metric.Description);
            }
            else
            {
                writer.WriteString(descriptionPropertyName, "");
            }
            writer.WriteString(timePropertyName, metricEventInstance.EventTime.ToString(postgreSQLTimestampFormat));
        }

        /// <summary>
        /// Attempts to execute a stored procedure which does not return a result set.
        /// </summary>
        /// <param name="procedureName">The name of the stored procedure.</param>
        /// <param name="parameters">The parameters to pass to the stored procedure.</param>
        protected void ExecuteStoredProcedure(String procedureName, IList<NpgsqlParameter> parameters)
        {
            var parameterStringBuilder = new StringBuilder();
            for (Int32 i = 0; i < parameters.Count; i++)
            {
                parameterStringBuilder.Append($"${i + 1}");
                if (i != parameters.Count - 1)
                {
                    parameterStringBuilder.Append(", ");
                }
            }
            String commandText = $"CALL {procedureName}({parameterStringBuilder.ToString()});";

            try
            {
                using (NpgsqlConnection connection = dataSource.OpenConnection())
                using (var command = new NpgsqlCommand(commandText))
                {
                    command.Connection = connection;
                    command.CommandTimeout = commandTimeout;
                    foreach (NpgsqlParameter currentParameter in parameters)
                    {
                        command.Parameters.Add(currentParameter);
                    }
                    command.ExecuteNonQuery();
                    connection.Close();
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Failed to execute stored procedure '{procedureName}' in PostgreSQL.", e);
            }
        }

        /// <summary>
        /// Creates an <see cref="NpgsqlParameter"/>
        /// </summary>
        /// <param name="parameterType">The type of the parameter.</param>
        /// <param name="parameterValue">The value of the parameter.</param>
        /// <returns>The created parameter.</returns>
        protected NpgsqlParameter CreateNpgsqlParameterWithValue(NpgsqlDbType parameterType, Object parameterValue)
        {
            var returnParameter = new NpgsqlParameter();
            returnParameter.NpgsqlDbType = parameterType;
            returnParameter.Value = parameterValue;

            return returnParameter;
        }

        #endregion

        #region Finalize / Dispose Methods

        /// <summary>
        /// Provides a method to free unmanaged resources used by this class.
        /// </summary>
        /// <param name="disposing">Whether the method is being called as part of an explicit Dispose routine, and hence whether managed resources should also be freed.</param>
        protected override void Dispose(bool disposing)
        {
            if (!disposed)
            {
                try
                {
                    if (disposing)
                    {
                        // Free other state (managed objects).
                        if (parallelProcessStartSignal != null)
                        {
                            parallelProcessStartSignal.Dispose();
                        }
                        if (parallelProcessCompletedSignal != null)
                        {
                            parallelProcessCompletedSignal.Dispose();
                        }
                        if (dataSource != null)
                        {
                            dataSource.Dispose();
                        }
                    }
                    // Free your own state (unmanaged objects).

                    // Set large fields to null.
                }
                finally
                {
                    base.Dispose(disposing);
                }
            }
        }

        #endregion

        #region Inner Classes

        /// <summary>
        /// Implementation of IStoredProcedureExecutionWrapper which allows executing stored procedures through a configurable <see cref="Action"/>.
        /// </summary>
        protected class StoredProcedureExecutionWrapper : IStoredProcedureExecutionWrapper
        {
            /// <summary>An action which executes the stored procedure.</summary>
            protected Action<String, IList<NpgsqlParameter>> executeAction;

            /// <summary>
            /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.PostgreSQL.PostgreSqlMetricLogger+StoredProcedureExecutionWrapper class.
            /// </summary>
            /// <param name="executeAction">An action which executes the stored procedure.</param>
            public StoredProcedureExecutionWrapper(Action<String, IList<NpgsqlParameter>> executeAction)
            {
                this.executeAction = executeAction;
            }

            /// <summary>
            /// Executes a stored procedure which does not return a result set.
            /// </summary>
            /// <param name="procedureName">The name of the stored procedure.</param>
            /// <param name="parameters">The parameters to pass to the stored procedure.</param>
            public void Execute(String procedureName, IList<NpgsqlParameter> parameters)
            {
                executeAction.Invoke(procedureName, parameters);
            }
        }

        /// <summary>
        /// Implementation of <see cref="IMetricLogger"/> which does not log.
        /// </summary>
        class NullLogger : IApplicationLogger
        {
            public void Log(LogLevel level, String text)
            {
            }

            public void Log(Object source, LogLevel level, String text)
            {
            }

            public void Log(Int32 eventIdentifier, LogLevel level, String text)
            {
            }

            public void Log(Object source, Int32 eventIdentifier, LogLevel level, String text)
            {
            }

            public void Log(LogLevel level, String text, Exception sourceException)
            {
            }

            public void Log(Object source, LogLevel level, String text, Exception sourceException)
            {
            }

            public void Log(Int32 eventIdentifier, LogLevel level, String text, Exception sourceException)
            {
            }

            public void Log(Object source, Int32 eventIdentifier, LogLevel level, String text, Exception sourceException)
            {
            }
        }

        #endregion
    }
}
