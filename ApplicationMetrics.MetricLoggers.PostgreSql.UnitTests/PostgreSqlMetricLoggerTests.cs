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
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using ApplicationLogging;
using StandardAbstraction;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;
using NUnit.Framework;
using NUnit.Framework.Internal;
using NUnit.Framework.Legacy;
using NSubstitute;

namespace ApplicationMetrics.MetricLoggers.PostgreSql.UnitTests
{
    /// <summary>
    /// Unit tests for the ApplicationMetrics.MetricLoggers.PostgreSql.PostgreSqlMetricLogger class.
    /// </summary>
    public class PostgreSqlMetricLoggerTests
    {
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
        protected const String postgreSQLTimestampFormat = "yyyy-MM-dd HH:mm:ss.ffffff";

        private string testCategory;
        private string testConnectionString;
        private IBufferProcessingStrategy mockBufferProcessingStrategy;
        private IApplicationLogger mockLogger;
        private IDateTime mockDateTimeProvider;
        private IStopwatch mockStopwatch;
        private IGuidProvider mockGuidProvider;
        private IStoredProcedureExecutionWrapper mockStoredProcedureExecutionWrapper;
        private PostgreSqlMetricLoggerWithProtectedMembers testPostgreSqlMetricLogger;

        [SetUp]
        protected void SetUp()
        {
            testCategory = "TestCategory";
            testConnectionString = "User ID=userId; Password=password; Host=testServer; Database=testDB;";

            mockBufferProcessingStrategy = Substitute.For<IBufferProcessingStrategy>();
            mockLogger = Substitute.For<IApplicationLogger>();
            mockDateTimeProvider = Substitute.For<IDateTime>();
            mockStopwatch = Substitute.For<IStopwatch>();
            mockStopwatch.Frequency.Returns<Int64>(10000000);
            mockGuidProvider = Substitute.For<IGuidProvider>();
            mockStoredProcedureExecutionWrapper = Substitute.For<IStoredProcedureExecutionWrapper>();
            testPostgreSqlMetricLogger = new PostgreSqlMetricLoggerWithProtectedMembers(testCategory, testConnectionString, 60, mockBufferProcessingStrategy, IntervalMetricBaseTimeUnit.Millisecond, true, mockLogger, mockDateTimeProvider, mockStopwatch, mockGuidProvider, mockStoredProcedureExecutionWrapper);
        }

        [TearDown]
        protected void TearDown()
        {
            testPostgreSqlMetricLogger.Dispose();
        }

        [Test]
        public void Constructor_CategoryStringParameterWhitespace()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testPostgreSqlMetricLogger = new PostgreSqlMetricLogger(" ", "User ID=userId; Password=password; Host=testServer; Database=testDB;", 60, mockBufferProcessingStrategy, IntervalMetricBaseTimeUnit.Millisecond, true);
            });

            Assert.That(e.Message, Does.StartWith($"Parameter 'category' must contain a value."));
            ClassicAssert.AreEqual("category", e.ParamName);
        }

        [Test]
        public void Constructor_ConnectionStringParameterWhitespace()
        {
            var e = Assert.Throws<ArgumentException>(delegate
            {
                var testPostgreSqlMetricLogger = new PostgreSqlMetricLogger("TestCategory", " ", 60, mockBufferProcessingStrategy, IntervalMetricBaseTimeUnit.Millisecond, true);
            });

            Assert.That(e.Message, Does.StartWith($"Parameter 'connectionString' must contain a value."));
            ClassicAssert.AreEqual("connectionString", e.ParamName);
        }

        [Test]
        public void Constructor_OperationTimeoutParameterLessThan0()
        {
            var e = Assert.Throws<ArgumentOutOfRangeException>(delegate
            {
                var testPostgreSqlMetricLogger = new PostgreSqlMetricLogger("TestCategory", "User ID=userId; Password=password; Host=testServer; Database=testDB;", -1, mockBufferProcessingStrategy, IntervalMetricBaseTimeUnit.Millisecond, true);
            });

            Assert.That(e.Message, Does.StartWith($"Parameter 'commandTimeout' with value -1 cannot be less than 0."));
            ClassicAssert.AreEqual("commandTimeout", e.ParamName);
        }

        [Test]
        public void ProcessCountMetricEvents_ExceptionExecutingStoredProcedure()
        {
            string mockExceptionMessage = "Mock PostgreSQL exception";
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertCountMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });

            // The first call will catch the exception on a worker thread, on the second call it will be re-thrown on the main thread
            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            var e = Assert.Throws<AggregateException>(delegate
            {
                SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            });

            Assert.That(e.Message, Does.StartWith($"One or more exceptions occurred on worker threads whilst writing metrics to PostgreSQL."));
            ClassicAssert.AreEqual(1, e.InnerExceptions.Count);
            var innerException = e.InnerExceptions[0];
            Assert.That(innerException.Message, Does.StartWith("An error occurred writing count metrics to PostgreSQL."));
            Assert.That(innerException.InnerException.Message, Does.StartWith(mockExceptionMessage));
        }
        [Test]
        public void ProcessCountMetricEvents_ExceptionExecutingStoredProcedureLoggingTest()
        {
            System.DateTime testStartTime = GenerateUtcDateTime("2022-12-23 21:00:00.000");
            string mockExceptionMessage = "Mock PostgreSQL exception";
            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsLoggingTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);
            mockDateTimeProvider.UtcNow.Returns(testStartTime);
            mockStopwatch.ElapsedTicks.Returns
            (
                ConvertMilliseondsToTicks(500),
                ConvertMilliseondsToTicks(750)
            );
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertCountMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });
            testPostgreSqlMetricLogger.Start();

            // The first call will catch the exception on a worker thread, on the second call it will be re-thrown on the main thread
            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, countMetricEventInstances, amountMetricEventInstances, statusMetricEventInstances, intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 11 metric events in 250 milliseconds.");
        }

        [Test]
        public void ProcessAmountMetricEvents_ExceptionExecutingStoredProcedure()
        {
            string mockExceptionMessage = "Mock PostgreSQL exception";
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertAmountMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });

            // The first call will catch the exception on a worker thread, on the second call it will be re-thrown on the main thread
            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            var e = Assert.Throws<AggregateException>(delegate
            {
                SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            });

            Assert.That(e.Message, Does.StartWith($"One or more exceptions occurred on worker threads whilst writing metrics to PostgreSQL."));
            ClassicAssert.AreEqual(1, e.InnerExceptions.Count);
            var innerException = e.InnerExceptions[0];
            Assert.That(innerException.Message, Does.StartWith("An error occurred writing amount metrics to PostgreSQL."));
            Assert.That(innerException.InnerException.Message, Does.StartWith(mockExceptionMessage));
        }
        [Test]
        public void ProcessAmountMetricEvents_ExceptionExecutingStoredProcedureLoggingTest()
        {
            System.DateTime testStartTime = GenerateUtcDateTime("2022-12-23 21:00:00.000");
            string mockExceptionMessage = "Mock PostgreSQL exception";
            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsLoggingTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);
            mockDateTimeProvider.UtcNow.Returns(testStartTime);
            mockStopwatch.ElapsedTicks.Returns
            (
                ConvertMilliseondsToTicks(500),
                ConvertMilliseondsToTicks(750)
            );
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertAmountMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });
            testPostgreSqlMetricLogger.Start();

            // The first call will catch the exception on a worker thread, on the second call it will be re-thrown on the main thread
            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, countMetricEventInstances, amountMetricEventInstances, statusMetricEventInstances, intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 10 metric events in 250 milliseconds.");
        }

        [Test]
        public void ProcessStatusMetricEvents_ExceptionExecutingStoredProcedure()
        {
            string mockExceptionMessage = "Mock PostgreSQL exception";
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertStatusMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });

            // The first call will catch the exception on a worker thread, on the second call it will be re-thrown on the main thread
            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            var e = Assert.Throws<AggregateException>(delegate
            {
                SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            });

            Assert.That(e.Message, Does.StartWith($"One or more exceptions occurred on worker threads whilst writing metrics to PostgreSQL."));
            ClassicAssert.AreEqual(1, e.InnerExceptions.Count);
            var innerException = e.InnerExceptions[0];
            Assert.That(innerException.Message, Does.StartWith("An error occurred writing status metrics to PostgreSQL."));
            Assert.That(innerException.InnerException.Message, Does.StartWith(mockExceptionMessage));
        }
        [Test]
        public void ProcessStatusMetricEvents_ExceptionExecutingStoredProcedureLoggingTest()
        {
            System.DateTime testStartTime = GenerateUtcDateTime("2022-12-23 21:00:00.000");
            string mockExceptionMessage = "Mock PostgreSQL exception";
            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsLoggingTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);
            mockDateTimeProvider.UtcNow.Returns(testStartTime);
            mockStopwatch.ElapsedTicks.Returns
            (
                ConvertMilliseondsToTicks(500),
                ConvertMilliseondsToTicks(750)
            );
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertStatusMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });
            testPostgreSqlMetricLogger.Start();

            // The first call will catch the exception on a worker thread, on the second call it will be re-thrown on the main thread
            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, countMetricEventInstances, amountMetricEventInstances, statusMetricEventInstances, intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 8 metric events in 250 milliseconds.");
        }

        [Test]
        public void ProcessIntervalMetricEvents_ExceptionExecutingStoredProcedure()
        {
            string mockExceptionMessage = "Mock PostgreSQL exception";
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertIntervalMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });

            // The first call will catch the exception on a worker thread, on the second call it will be re-thrown on the main thread
            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            var e = Assert.Throws<AggregateException>(delegate
            {
                SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            });

            Assert.That(e.Message, Does.StartWith($"One or more exceptions occurred on worker threads whilst writing metrics to PostgreSQL."));
            ClassicAssert.AreEqual(1, e.InnerExceptions.Count);
            var innerException = e.InnerExceptions[0];
            Assert.That(innerException.Message, Does.StartWith("An error occurred writing interval metrics to PostgreSQL."));
            Assert.That(innerException.InnerException.Message, Does.StartWith(mockExceptionMessage));
        }
        [Test]
        public void ProcessIntervalMetricEvents_ExceptionExecutingStoredProcedureLoggingTest()
        {
            System.DateTime testStartTime = GenerateUtcDateTime("2022-12-23 21:00:00.000");
            string mockExceptionMessage = "Mock PostgreSQL exception";
            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsLoggingTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);
            mockDateTimeProvider.UtcNow.Returns(testStartTime);
            mockStopwatch.ElapsedTicks.Returns
            (
                ConvertMilliseondsToTicks(500),
                ConvertMilliseondsToTicks(750)
            );
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertIntervalMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });
            testPostgreSqlMetricLogger.Start();

            // The first call will catch the exception on a worker thread, on the second call it will be re-thrown on the main thread
            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, countMetricEventInstances, amountMetricEventInstances, statusMetricEventInstances, intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 7 metric events in 250 milliseconds.");
        }

        [Test]
        public void DequeueAndProcessMetricEvents_ExceptionExecutingStoredProceduresOnAllProcessMethods()
        {
            // Tests that multiple exceptions occurring on worker threads are re-thrown as a cobined AggregateException 
            string mockExceptionMessage = "Mock PostgreSQL exception";
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertCountMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertAmountMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertStatusMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });
            mockStoredProcedureExecutionWrapper.When(wrapper => wrapper.Execute(insertIntervalMetricsStoredProcedureName, Arg.Any<IList<NpgsqlParameter>>())).Do(callInfo => { throw new Exception(mockExceptionMessage); });

            // The first call will catch the exception on a worker thread, on the second call it will be re-thrown on the main thread
            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            var e = Assert.Throws<AggregateException>(delegate
            {
                SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, null, null, null, null);
            });

            Assert.That(e.Message, Does.StartWith($"One or more exceptions occurred on worker threads whilst writing metrics to PostgreSQL."));
            ClassicAssert.AreEqual(4, e.InnerExceptions.Count);
            var allInnerExceptions = new List<Exception>(e.InnerExceptions);
            allInnerExceptions.Sort(delegate (Exception x, Exception y)
            {
                return x.Message.CompareTo(y.Message);
            });
            Assert.That(allInnerExceptions[0].Message, Does.StartWith("An error occurred writing amount metrics to PostgreSQL."));
            Assert.That(allInnerExceptions[1].Message, Does.StartWith("An error occurred writing count metrics to PostgreSQL."));
            Assert.That(allInnerExceptions[2].Message, Does.StartWith("An error occurred writing interval metrics to PostgreSQL."));
            Assert.That(allInnerExceptions[3].Message, Does.StartWith("An error occurred writing status metrics to PostgreSQL."));
        }

        [Test]
        public void DequeueAndProcessMetricEvents()
        {
            // Variables to capture the table-valued parameters passed to each of the stored procedures
            List<NpgsqlParameter> countMetricProcedureParameters = null;
            JArray countMetricProcedureJsonParameter = null;
            List<NpgsqlParameter> amountMetricProcedureParameters = null;
            JArray amountMetricProcedureJsonParameter = null;
            List<NpgsqlParameter> statusMetricProcedureParameters = null;
            JArray statusMetricProcedureJsonParameter = null;
            List<NpgsqlParameter> intervalMetricProcedureParameters = null;
            JArray intervalMetricProcedureJsonParameter = null;

            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsSuccessTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);

            mockStoredProcedureExecutionWrapper.Execute(insertCountMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>
            (
                (IList<NpgsqlParameter> parameters) =>
                {
                    countMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        countMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));
            mockStoredProcedureExecutionWrapper.Execute(insertAmountMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>(
                (IList<NpgsqlParameter> parameters) =>
                {
                    amountMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        amountMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));
            mockStoredProcedureExecutionWrapper.Execute(insertStatusMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>(
                (IList<NpgsqlParameter> parameters) =>
                {
                    statusMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        statusMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));
            mockStoredProcedureExecutionWrapper.Execute(insertIntervalMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>(
                (IList<NpgsqlParameter> parameters) =>
                {
                    intervalMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        intervalMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));

            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, countMetricEventInstances, amountMetricEventInstances, statusMetricEventInstances, intervalMetricEventInstances);

            AssertDequeueAndProcessMetricEventsSuccessTestStoredProcedureParameters
            (
                countMetricProcedureParameters,
                countMetricProcedureJsonParameter, 
                amountMetricProcedureParameters,
                amountMetricProcedureJsonParameter, 
                statusMetricProcedureParameters,
                statusMetricProcedureJsonParameter, 
                intervalMetricProcedureParameters,
                intervalMetricProcedureJsonParameter
            );
        }

        [Test]
        public void DequeueAndProcessMetricEvents_LoggingTest()
        {
            System.DateTime testStartTime = GenerateUtcDateTime("2022-12-23 21:00:00.000");
            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsLoggingTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);
            mockDateTimeProvider.UtcNow.Returns(testStartTime);
            mockStopwatch.ElapsedTicks.Returns
            (
                ConvertMilliseondsToTicks(500),
                ConvertMilliseondsToTicks(750)
            );
            testPostgreSqlMetricLogger.Start();

            SimulateDequeueAndProcessMetricEventsMethod(testPostgreSqlMetricLogger, countMetricEventInstances, amountMetricEventInstances, statusMetricEventInstances, intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 12 metric events in 250 milliseconds.");
        }

        [Test]
        public void DequeueAndProcessMetricEvents_WorkerThreadsStartedInReverseOrder()
        {
            // Variables to capture the table-valued parameters passed to each of the stored procedures
            List<NpgsqlParameter> countMetricProcedureParameters = null;
            JArray countMetricProcedureJsonParameter = null;
            List<NpgsqlParameter> amountMetricProcedureParameters = null;
            JArray amountMetricProcedureJsonParameter = null;
            List<NpgsqlParameter> statusMetricProcedureParameters = null;
            JArray statusMetricProcedureJsonParameter = null;
            List<NpgsqlParameter> intervalMetricProcedureParameters = null;
            JArray intervalMetricProcedureJsonParameter = null;

            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsSuccessTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);

            mockStoredProcedureExecutionWrapper.Execute(insertCountMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>
                        (
                            (IList<NpgsqlParameter> parameters) =>
                            {
                                countMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                                using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                                {
                                    countMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                                }
                            }
                        ));
            mockStoredProcedureExecutionWrapper.Execute(insertAmountMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>(
                (IList<NpgsqlParameter> parameters) =>
                {
                    amountMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        amountMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));
            mockStoredProcedureExecutionWrapper.Execute(insertStatusMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>(
                (IList<NpgsqlParameter> parameters) =>
                {
                    statusMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        statusMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));
            mockStoredProcedureExecutionWrapper.Execute(insertIntervalMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>(
                (IList<NpgsqlParameter> parameters) =>
                {
                    intervalMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        intervalMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));

            // Call the 4 Process*MetricEvents() methods in semi-reverse order to ensure that thread signals still work properly when thread are started in an unexpected order
            //   We're simulating the indeterministic start order of the code within the Tasks created by each of the first 3 Process*MetricEvents() but changing the call order of the methods (although ofcourse in the real case the call order will not vary)
            //   Only caveat is that the 'last' method ProcessIntervalMetricEvents() runs on the main thread rather than creating a Task, and starts by waiting on a signal, so this method must always be called last
            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            // Wait to try and ensure the worker threads start in the order specified by the method calls
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            AssertDequeueAndProcessMetricEventsSuccessTestStoredProcedureParameters
            (
                countMetricProcedureParameters,
                countMetricProcedureJsonParameter,
                amountMetricProcedureParameters,
                amountMetricProcedureJsonParameter,
                statusMetricProcedureParameters,
                statusMetricProcedureJsonParameter,
                intervalMetricProcedureParameters,
                intervalMetricProcedureJsonParameter
            );
        }
        
        [Test]
        public void DequeueAndProcessMetricEvents_WorkerThreadsStartedInReverseOrderLoggingTest()
        {
            System.DateTime testStartTime = GenerateUtcDateTime("2022-12-23 21:00:00.000");
            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsLoggingTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);
            mockDateTimeProvider.UtcNow.Returns(testStartTime);
            mockStopwatch.ElapsedTicks.Returns
            (
                ConvertMilliseondsToTicks(500),
                ConvertMilliseondsToTicks(750)
            );
            testPostgreSqlMetricLogger.Start();

            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 12 metric events in 250 milliseconds.");
        }
        
        [Test]
        public void DequeueAndProcessMetricEvents_WorkerThreadsStartedInRandomOrders()
        {
            // Variables to capture the table-valued parameters passed to each of the stored procedures
            List<NpgsqlParameter> countMetricProcedureParameters = null;
            JArray countMetricProcedureJsonParameter = null;
            List<NpgsqlParameter> amountMetricProcedureParameters = null;
            JArray amountMetricProcedureJsonParameter = null;
            List<NpgsqlParameter> statusMetricProcedureParameters = null;
            JArray statusMetricProcedureJsonParameter = null;
            List<NpgsqlParameter> intervalMetricProcedureParameters = null;
            JArray intervalMetricProcedureJsonParameter = null;

            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsSuccessTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);

            mockStoredProcedureExecutionWrapper.Execute(insertCountMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>
                                    (
                                        (IList<NpgsqlParameter> parameters) =>
                                        {
                                            countMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                                            using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                                            {
                                                countMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                                            }
                                        }
                                    ));
            mockStoredProcedureExecutionWrapper.Execute(insertAmountMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>(
                (IList<NpgsqlParameter> parameters) =>
                {
                    amountMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        amountMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));
            mockStoredProcedureExecutionWrapper.Execute(insertStatusMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>(
                (IList<NpgsqlParameter> parameters) =>
                {
                    statusMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        statusMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));
            mockStoredProcedureExecutionWrapper.Execute(insertIntervalMetricsStoredProcedureName, Arg.Do<IList<NpgsqlParameter>>(
                (IList<NpgsqlParameter> parameters) =>
                {
                    intervalMetricProcedureParameters = new List<NpgsqlParameter>(parameters);
                    using (JsonDocument eventsJson = (JsonDocument)parameters[1].Value)
                    {
                        intervalMetricProcedureJsonParameter = JArray.Parse(eventsJson.RootElement.ToString());
                    }
                }
            ));

            // Call the 4 Process*MetricEvents() methods in random orders order to ensure that thread signals still work properly when thread are started in an unexpected order
            //   See comments/caveat in DequeueAndProcessMetricEvents_WorkerThreadsStartedInReverseOrder() test
            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            AssertDequeueAndProcessMetricEventsSuccessTestStoredProcedureParameters
            (
                countMetricProcedureParameters,
                countMetricProcedureJsonParameter,
                amountMetricProcedureParameters,
                amountMetricProcedureJsonParameter,
                statusMetricProcedureParameters,
                statusMetricProcedureJsonParameter,
                intervalMetricProcedureParameters,
                intervalMetricProcedureJsonParameter
            );


            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            AssertDequeueAndProcessMetricEventsSuccessTestStoredProcedureParameters
            (
                countMetricProcedureParameters,
                countMetricProcedureJsonParameter,
                amountMetricProcedureParameters,
                amountMetricProcedureJsonParameter,
                statusMetricProcedureParameters,
                statusMetricProcedureJsonParameter,
                intervalMetricProcedureParameters,
                intervalMetricProcedureJsonParameter
            );


            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            AssertDequeueAndProcessMetricEventsSuccessTestStoredProcedureParameters
            (
                countMetricProcedureParameters,
                countMetricProcedureJsonParameter,
                amountMetricProcedureParameters,
                amountMetricProcedureJsonParameter,
                statusMetricProcedureParameters,
                statusMetricProcedureJsonParameter,
                intervalMetricProcedureParameters,
                intervalMetricProcedureJsonParameter
            );


            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            AssertDequeueAndProcessMetricEventsSuccessTestStoredProcedureParameters
            (
                countMetricProcedureParameters,
                countMetricProcedureJsonParameter,
                amountMetricProcedureParameters,
                amountMetricProcedureJsonParameter,
                statusMetricProcedureParameters,
                statusMetricProcedureJsonParameter,
                intervalMetricProcedureParameters,
                intervalMetricProcedureJsonParameter
            );
        }
        
        [Test]
        public void DequeueAndProcessMetricEvents_WorkerThreadsStartedInRandomOrdersLoggingTest()
        {
            System.DateTime testStartTime = GenerateUtcDateTime("2022-12-23 21:00:00.000");
            List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances;
            List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances;
            List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances;
            List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances;
            GenerateDequeueAndProcessMetricEventsLoggingTestParameters(out countMetricEventInstances, out amountMetricEventInstances, out statusMetricEventInstances, out intervalMetricEventInstances);
            mockDateTimeProvider.UtcNow.Returns(testStartTime);
            mockStopwatch.ElapsedTicks.Returns
            (
                ConvertMilliseondsToTicks(0),
                ConvertMilliseondsToTicks(250),
                ConvertMilliseondsToTicks(1000),
                ConvertMilliseondsToTicks(1249),
                ConvertMilliseondsToTicks(2000),
                ConvertMilliseondsToTicks(2248),
                ConvertMilliseondsToTicks(3000),
                ConvertMilliseondsToTicks(3247)
            );
            testPostgreSqlMetricLogger.Start();

            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 12 metric events in 250 milliseconds.");


            mockLogger.ClearReceivedCalls();

            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 12 metric events in 249 milliseconds.");


            mockLogger.ClearReceivedCalls();

            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 12 metric events in 248 milliseconds.");


            mockLogger.ClearReceivedCalls();

            testPostgreSqlMetricLogger.ProcessStatusMetricEvents(statusMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessCountMetricEvents(countMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessAmountMetricEvents(amountMetricEventInstances);
            Thread.Sleep(250);
            testPostgreSqlMetricLogger.ProcessIntervalMetricEvents(intervalMetricEventInstances);

            mockLogger.Received(1).Log(testPostgreSqlMetricLogger, LogLevel.Information, "Processed 12 metric events in 247 milliseconds.");
        }

        #region Private/Protected Methods

        /// <summary>
        /// Simulates calling the protected MetricLoggerBuffer.DequeueAndProcessMetricEvents() method.
        /// </summary>
        /// <param name="PostgreSqlMetricLoggerInstance">The <see cref="PostgreSqlMetricLoggerWithProtectedMembers"/> instance to simulate calling the method on.</param>
        /// <param name="countMetricEvents">Parameters representing a set of CountMetricEventInstance classes.</param>
        /// <param name="amountMetricEvents">Parameters representing a set of AmountMetricEventInstance classes.</param>
        /// <param name="statusMetricEvents">Parameters representing a set of StatusMetricEventInstance classes.</param>
        /// <param name="intervalMetricEvents">Parameters representing a set of IntervalMetricEventInstance classes.</param>
        private void SimulateDequeueAndProcessMetricEventsMethod
        (
            PostgreSqlMetricLoggerWithProtectedMembers PostgreSqlMetricLoggerInstance,
            IEnumerable<Tuple<CountMetric, System.DateTime>> countMetricEvents,
            IEnumerable<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEvents,
            IEnumerable<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEvents,
            IEnumerable<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEvents
        )
        {
            if (countMetricEvents == null)
            {
                PostgreSqlMetricLoggerInstance.ProcessCountMetricEvents(Enumerable.Empty<Tuple<CountMetric, System.DateTime>>());
            }
            else
            {
                PostgreSqlMetricLoggerInstance.ProcessCountMetricEvents(countMetricEvents);
            }
            if (amountMetricEvents == null)
            {
                PostgreSqlMetricLoggerInstance.ProcessAmountMetricEvents(Enumerable.Empty<Tuple<AmountMetric, Int64, System.DateTime>>());
            }
            else
            {
                PostgreSqlMetricLoggerInstance.ProcessAmountMetricEvents(amountMetricEvents);
            }
            if (statusMetricEvents == null)
            {
                PostgreSqlMetricLoggerInstance.ProcessStatusMetricEvents(Enumerable.Empty<Tuple<StatusMetric, Int64, System.DateTime>>());
            }
            else
            {
                PostgreSqlMetricLoggerInstance.ProcessStatusMetricEvents(statusMetricEvents);
            }
            if (intervalMetricEvents == null)
            {
                PostgreSqlMetricLoggerInstance.ProcessIntervalMetricEvents(Enumerable.Empty<Tuple<IntervalMetric, Int64, System.DateTime>>());
            }
            else
            {
                PostgreSqlMetricLoggerInstance.ProcessIntervalMetricEvents(intervalMetricEvents);
            }
        }

        /// <summary>
        /// Generates as UTC <see cref="System.DateTime"/> from the specified string containing a date in ISO format.
        /// </summary>
        /// <param name="isoFormattedDateString">The date string.</param>
        /// <returns>the DateTime.</returns>
        private System.DateTime GenerateUtcDateTime(String isoFormattedDateString)
        {
            var returnDateTime = System.DateTime.ParseExact(isoFormattedDateString, "yyyy-MM-dd HH:mm:ss.fff", DateTimeFormatInfo.InvariantInfo);
            return System.DateTime.SpecifyKind(returnDateTime, DateTimeKind.Utc);
        }

        /// <summary>
        /// Converts the specified number of milliseonds to ticks.
        /// </summary>
        /// <param name="millisecondValue">The millisecond value to convert.</param>
        /// <returns>The millisecond value in ticks.</returns>
        private Int32 ConvertMilliseondsToTicks(Int32 millisecondValue)
        {
            return millisecondValue * 10000;
        }

        /// <summary>
        /// Sets up parameters for testing the DequeueAndProcessMetricEvents() method (via method SimulateDequeueAndProcessMetricEventsMethod() in this test class).
        /// </summary>
        private void GenerateDequeueAndProcessMetricEventsSuccessTestParameters
        (
            out List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances,
            out List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances,
            out List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances,
            out List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances
        )
        {
            countMetricEventInstances = new List<Tuple<CountMetric, System.DateTime>>()
            {
                new Tuple<CountMetric, System.DateTime>(new DiskReadOperation(), GenerateUtcDateTime("2022-08-30 21:58:00.001")),
                new Tuple<CountMetric, System.DateTime>(new DiskReadOperation(), GenerateUtcDateTime("2022-08-30 21:58:00.002")),
                new Tuple<CountMetric, System.DateTime>(new MessageReceived(), GenerateUtcDateTime("2022-08-30 21:58:00.003")),
                new Tuple<CountMetric, System.DateTime>(new MessageReceived(), GenerateUtcDateTime("2022-08-30 21:58:00.004"))
            };
            amountMetricEventInstances = new List<Tuple<AmountMetric, Int64, System.DateTime>>()
            {
                new Tuple<AmountMetric, Int64, System.DateTime>(new DiskBytesRead(), 1, GenerateUtcDateTime("2022-08-30 21:58:00.005")),
                new Tuple<AmountMetric, Int64, System.DateTime>(new MessageSize(), 2, GenerateUtcDateTime("2022-08-30 21:58:00.006")),
                new Tuple<AmountMetric, Int64, System.DateTime>(new DiskBytesRead(), 3, GenerateUtcDateTime("2022-08-30 21:58:00.007")),
                new Tuple<AmountMetric, Int64, System.DateTime>(new MessageSize(), 4, GenerateUtcDateTime("2022-08-30 21:58:00.008"))
            };
            statusMetricEventInstances = new List<Tuple<StatusMetric, Int64, System.DateTime>>()
            {
                new Tuple<StatusMetric, Int64, System.DateTime>(new AvailableMemory(), 5, GenerateUtcDateTime("2022-08-30 21:58:00.009")),
                new Tuple<StatusMetric, Int64, System.DateTime>(new AvailableMemory(), 6, GenerateUtcDateTime("2022-08-30 21:58:00.010")),
                new Tuple<StatusMetric, Int64, System.DateTime>(new ActiveWorkerThreads(), 7, GenerateUtcDateTime("2022-08-30 21:58:00.011")),
                new Tuple<StatusMetric, Int64, System.DateTime>(new ActiveWorkerThreads(), 8, GenerateUtcDateTime("2022-08-30 21:58:00.012"))
            };
            intervalMetricEventInstances = new List<Tuple<IntervalMetric, Int64, System.DateTime>>()
            {
                new Tuple<IntervalMetric, Int64, System.DateTime>(new DiskReadTime(), 9, GenerateUtcDateTime("2022-08-30 21:58:00.013")),
                new Tuple<IntervalMetric, Int64, System.DateTime>(new MessageReceiveTime(), 10, GenerateUtcDateTime("2022-08-30 21:58:00.014")),
                new Tuple<IntervalMetric, Int64, System.DateTime>(new DiskReadTime(), 11, GenerateUtcDateTime("2022-08-30 21:58:00.015")),
                new Tuple<IntervalMetric, Int64, System.DateTime>(new MessageReceiveTime(), 12, GenerateUtcDateTime("2022-08-30 21:58:00.016")),
            };
        }

        /// <summary>
        /// Sets up parameters for testing logging in the DequeueAndProcessMetricEvents() method (via method SimulateDequeueAndProcessMetricEventsMethod() in this test class).
        /// </summary>
        private void GenerateDequeueAndProcessMetricEventsLoggingTestParameters
        (
            out List<Tuple<CountMetric, System.DateTime>> countMetricEventInstances,
            out List<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEventInstances,
            out List<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEventInstances,
            out List<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEventInstances
        )
        {
            countMetricEventInstances = new List<Tuple<CountMetric, System.DateTime>>()
            {
                new Tuple<CountMetric, System.DateTime>(new DiskReadOperation(), System.DateTime.UtcNow)
            };
            amountMetricEventInstances = new List<Tuple<AmountMetric, Int64, System.DateTime>>()
            {
                new Tuple<AmountMetric, Int64, System.DateTime>(new DiskBytesRead(), 1, System.DateTime.UtcNow),
                new Tuple<AmountMetric, Int64, System.DateTime>(new DiskBytesRead(), 1, System.DateTime.UtcNow)
            };
            statusMetricEventInstances = new List<Tuple<StatusMetric, Int64, System.DateTime>>()
            {
                new Tuple<StatusMetric, Int64, System.DateTime>(new AvailableMemory(), 2, System.DateTime.UtcNow),
                new Tuple<StatusMetric, Int64, System.DateTime>(new AvailableMemory(), 2, System.DateTime.UtcNow),
                new Tuple<StatusMetric, Int64, System.DateTime>(new AvailableMemory(), 2, System.DateTime.UtcNow),
                new Tuple<StatusMetric, Int64, System.DateTime>(new AvailableMemory(), 2, System.DateTime.UtcNow)
            };
            intervalMetricEventInstances = new List<Tuple<IntervalMetric, Int64, System.DateTime>>()
            {
                new Tuple<IntervalMetric, Int64, System.DateTime>(new DiskReadTime(), 3, System.DateTime.UtcNow),
                new Tuple<IntervalMetric, Int64, System.DateTime>(new DiskReadTime(), 3, System.DateTime.UtcNow),
                new Tuple<IntervalMetric, Int64, System.DateTime>(new DiskReadTime(), 3, System.DateTime.UtcNow),
                new Tuple<IntervalMetric, Int64, System.DateTime>(new DiskReadTime(), 3, System.DateTime.UtcNow),
                new Tuple<IntervalMetric, Int64, System.DateTime>(new DiskReadTime(), 3, System.DateTime.UtcNow)
            };
        }

        /// <summary>
        /// Asserts correctness of collections of <see cref="SqlParameter"/> objects passed to stored procedures (and intercepted via mocks) as part of the DequeueAndProcessMetricEvents() method (via method SimulateDequeueAndProcessMetricEventsMethod() in this test class).
        /// </summary>
        private void AssertDequeueAndProcessMetricEventsSuccessTestStoredProcedureParameters
        (
            List<NpgsqlParameter> countMetricProcedureParameters,
            JArray countMetricProcedureJsonParameter, 
            List<NpgsqlParameter> amountMetricProcedureParameters,
            JArray amountMetricProcedureJsonParameter,
            List<NpgsqlParameter> statusMetricProcedureParameters,
            JArray statusMetricProcedureJsonParameter,
            List<NpgsqlParameter> intervalMetricProcedureParameters, 
            JArray intervalMetricProcedureJsonParameter
        )
        {
            // Check parameters to 'insert count metrics' stored procedure
            ClassicAssert.AreEqual(2, countMetricProcedureParameters.Count);
            ClassicAssert.AreEqual(NpgsqlDbType.Varchar, countMetricProcedureParameters[0].NpgsqlDbType);
            ClassicAssert.AreEqual(testCategory, countMetricProcedureParameters[0].Value);
            ClassicAssert.AreEqual(NpgsqlDbType.Json, countMetricProcedureParameters[1].NpgsqlDbType);
            ClassicAssert.AreEqual(4, countMetricProcedureJsonParameter.Count);
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[0], namePropertyName, new DiskReadOperation().Name);
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[0], descriptionPropertyName, new DiskReadOperation().Description);
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[0], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.001").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[1], namePropertyName, new DiskReadOperation().Name);
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[1], descriptionPropertyName, "");
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[1], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.002").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[2], namePropertyName, new MessageReceived().Name);
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[2], descriptionPropertyName, new MessageReceived().Description);
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[2], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.003").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[3], namePropertyName, new MessageReceived().Name);
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[3], descriptionPropertyName, "");
            AssertJsonArrayElementContainsStringPropertyValue(countMetricProcedureJsonParameter[3], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.004").ToString(postgreSQLTimestampFormat));

            // Check parameters to 'insert amount metrics' stored procedure
            ClassicAssert.AreEqual(2, amountMetricProcedureParameters.Count);
            ClassicAssert.AreEqual(NpgsqlDbType.Varchar, amountMetricProcedureParameters[0].NpgsqlDbType);
            ClassicAssert.AreEqual(testCategory, amountMetricProcedureParameters[0].Value);
            ClassicAssert.AreEqual(NpgsqlDbType.Json, amountMetricProcedureParameters[1].NpgsqlDbType);
            ClassicAssert.AreEqual(4, amountMetricProcedureJsonParameter.Count);
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[0], namePropertyName, new DiskBytesRead().Name);
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[0], descriptionPropertyName, new DiskBytesRead().Description);
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[0], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.005").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[0], amountPropertyName, "1");
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[1], namePropertyName, new MessageSize().Name);
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[1], descriptionPropertyName, new MessageSize().Description);
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[1], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.006").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[1], amountPropertyName, "2");
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[2], namePropertyName, new DiskBytesRead().Name);
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[2], descriptionPropertyName, "");
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[2], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.007").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[2], amountPropertyName, "3");
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[3], namePropertyName, new MessageSize().Name);
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[3], descriptionPropertyName, "");
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[3], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.008").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(amountMetricProcedureJsonParameter[3], amountPropertyName, "4");

            // Check parameters to 'insert status metrics' stored procedure
            ClassicAssert.AreEqual(2, statusMetricProcedureParameters.Count);
            ClassicAssert.AreEqual(NpgsqlDbType.Varchar, statusMetricProcedureParameters[0].NpgsqlDbType);
            ClassicAssert.AreEqual(testCategory, statusMetricProcedureParameters[0].Value);
            ClassicAssert.AreEqual(NpgsqlDbType.Json, statusMetricProcedureParameters[1].NpgsqlDbType);
            ClassicAssert.AreEqual(4, statusMetricProcedureJsonParameter.Count);
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[0], namePropertyName, new AvailableMemory().Name);
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[0], descriptionPropertyName, new AvailableMemory().Description);
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[0], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.009").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[0], valuePropertyName, "5");
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[1], namePropertyName, new AvailableMemory().Name);
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[1], descriptionPropertyName, "");
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[1], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.010").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[1], valuePropertyName, "6");
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[2], namePropertyName, new ActiveWorkerThreads().Name);
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[2], descriptionPropertyName, new ActiveWorkerThreads().Description);
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[2], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.011").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[2], valuePropertyName, "7");
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[3], namePropertyName, new ActiveWorkerThreads().Name);
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[3], descriptionPropertyName, "");
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[3], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.012").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(statusMetricProcedureJsonParameter[3], valuePropertyName, "8");

            // Check parameters to 'insert interval metrics' stored procedure
            ClassicAssert.AreEqual(2, intervalMetricProcedureParameters.Count);
            ClassicAssert.AreEqual(NpgsqlDbType.Varchar, intervalMetricProcedureParameters[0].NpgsqlDbType);
            ClassicAssert.AreEqual(testCategory, intervalMetricProcedureParameters[0].Value);
            ClassicAssert.AreEqual(NpgsqlDbType.Json, intervalMetricProcedureParameters[1].NpgsqlDbType);
            ClassicAssert.AreEqual(4, intervalMetricProcedureJsonParameter.Count);
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[0], namePropertyName, new DiskReadTime().Name);
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[0], descriptionPropertyName, new DiskReadTime().Description);
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[0], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.013").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[0], durationPropertyName, "9");
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[1], namePropertyName, new MessageReceiveTime().Name);
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[1], descriptionPropertyName, new MessageReceiveTime().Description);
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[1], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.014").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[1], durationPropertyName, "10");
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[2], namePropertyName, new DiskReadTime().Name);
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[2], descriptionPropertyName, "");
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[2], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.015").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[2], durationPropertyName, "11");
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[3], namePropertyName, new MessageReceiveTime().Name);
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[3], descriptionPropertyName, "");
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[3], timePropertyName, GenerateUtcDateTime("2022-08-30 21:58:00.016").ToString(postgreSQLTimestampFormat));
            AssertJsonArrayElementContainsStringPropertyValue(intervalMetricProcedureJsonParameter[3], durationPropertyName, "12");
        }

        /// <summary>
        /// Asserts that the specified JSON array element is a <see cref="JObject"/> and contains a string property with the specified name and value.
        /// </summary>
        /// <param name="arrayElement">The JSON array element.</param>
        /// <param name="propertyName">The name of the property.</param>
        /// <param name="propertyValue">The value of the property.</param>
        protected void AssertJsonArrayElementContainsStringPropertyValue(JToken arrayElement, String propertyName, String propertyValue)
        {
            ClassicAssert.IsInstanceOf<JObject>(arrayElement);
            var arrayElementAsJObject = (JObject)arrayElement;
            ClassicAssert.NotNull(arrayElementAsJObject[propertyName]);
            ClassicAssert.AreEqual(propertyValue, arrayElementAsJObject[propertyName].ToString());
        }

        #endregion

        #region Nested Classes

        /// <summary>
        /// Version of the PostgreSqlMetricLogger class where private and protected methods are exposed as public so that they can be unit tested.
        /// </summary>
        private class PostgreSqlMetricLoggerWithProtectedMembers : PostgreSqlMetricLogger
        {
            /// <summary>
            /// Initialises a new instance of the ApplicationMetrics.MetricLoggers.PostgreSql.UnitTests.PostgreSqlMetricLoggerTests+PostgreSqlMetricLoggerWithProtectedMembers class.
            /// </summary>
            /// <param name="category">The category to log all metrics under.</param>
            /// <param name="connectionString">The string to use to connect to the PostgreSQL database.</param>
            /// <param name="commandTimeout">The time in seconds to wait while trying to execute a command, before terminating the attempt and generating an error. Set to zero for infinity.</param>
            /// <param name="bufferProcessingStrategy">Object which implements a processing strategy for the buffers (queues).</param>
            /// <param name="intervalMetricBaseTimeUnit">The base time unit to use to log interval metrics.</param>
            /// <param name="intervalMetricChecking">Specifies whether an exception should be thrown if the correct order of interval metric logging is not followed (e.g. End() method called before Begin()).  Note that this parameter only has an effect when running in 'non-interleaved' mode.</param>
            /// <param name="logger">The logger to use for performance statistics.</param>
            /// <param name="dateTime">A test (mock) <see cref="System.DateTime"/> object.</param>
            /// <param name="stopWatch">A test (mock) <see cref="Stopwatch"/> object.</param>
            /// <param name="guidProvider">A test (mock) <see cref="IGuidProvider"/> object.</param>
            /// <param name="storedProcedureExecutor">A test (mock) <see cref="IStoredProcedureExecutionWrapper"/> object.</param>
            public PostgreSqlMetricLoggerWithProtectedMembers
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
            : base(category, connectionString, commandTimeout, bufferProcessingStrategy, intervalMetricBaseTimeUnit, intervalMetricChecking, logger, dateTime, stopWatch, guidProvider, storedProcedureExecutor)
            {
            }

            public void ProcessCountMetricEvents(IEnumerable<Tuple<CountMetric, System.DateTime>> countMetricEvents)
            {
                var countMetricEventsQueue = new Queue<CountMetricEventInstance>();
                foreach (Tuple<CountMetric, System.DateTime> currentCountMetricEvent in countMetricEvents)
                {
                    countMetricEventsQueue.Enqueue(new CountMetricEventInstance(currentCountMetricEvent.Item1, currentCountMetricEvent.Item2));
                }
                ProcessCountMetricEvents(countMetricEventsQueue);
            }

            public void ProcessAmountMetricEvents(IEnumerable<Tuple<AmountMetric, Int64, System.DateTime>> amountMetricEvents)
            {
                var amountMetricEventsQueue = new Queue<AmountMetricEventInstance>();
                foreach (Tuple<AmountMetric, Int64, System.DateTime> currentAmountMetricEvent in amountMetricEvents)
                {
                    amountMetricEventsQueue.Enqueue(new AmountMetricEventInstance(currentAmountMetricEvent.Item1, currentAmountMetricEvent.Item2, currentAmountMetricEvent.Item3));
                }
                ProcessAmountMetricEvents(amountMetricEventsQueue);
            }

            public void ProcessStatusMetricEvents(IEnumerable<Tuple<StatusMetric, Int64, System.DateTime>> statusMetricEvents)
            {
                var statusMetricEventsQueue = new Queue<StatusMetricEventInstance>();
                foreach (Tuple<StatusMetric, Int64, System.DateTime> currentStatusMetricEvent in statusMetricEvents)
                {
                    statusMetricEventsQueue.Enqueue(new StatusMetricEventInstance(currentStatusMetricEvent.Item1, currentStatusMetricEvent.Item2, currentStatusMetricEvent.Item3));
                }
                ProcessStatusMetricEvents(statusMetricEventsQueue);
            }

            public void ProcessIntervalMetricEvents(IEnumerable<Tuple<IntervalMetric, Int64, System.DateTime>> intervalMetricEvents)
            {
                var intervalMetricEventsQueue = new Queue<Tuple<IntervalMetricEventInstance, Int64>>();
                foreach (Tuple<IntervalMetric, Int64, System.DateTime> currentIntervalMetricEvent in intervalMetricEvents)
                {
                    intervalMetricEventsQueue.Enqueue(new Tuple<IntervalMetricEventInstance, Int64>(new IntervalMetricEventInstance(currentIntervalMetricEvent.Item1, IntervalMetricEventTimePoint.Start, currentIntervalMetricEvent.Item3), currentIntervalMetricEvent.Item2));
                }
                ProcessIntervalMetricEvents(intervalMetricEventsQueue);
            }
        }

        #endregion
    }
}
