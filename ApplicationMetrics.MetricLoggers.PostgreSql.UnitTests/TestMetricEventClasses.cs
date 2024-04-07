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
using ApplicationMetrics;

namespace ApplicationMetrics.MetricLoggers.PostgreSql.UnitTests
{
    class DiskReadOperation : CountMetric
    {
        protected static String staticName = "DiskReadOperation";
        protected static String staticDescription = "A disk read operation";

        public DiskReadOperation()
        {
            base.name = staticName;
            base.description = staticDescription;
        }
    }

    class MessageReceived : CountMetric
    {
        protected static String staticName = "MessageReceived";
        protected static String staticDescription = "A message was received";

        public MessageReceived()
        {
            base.name = staticName;
            base.description = staticDescription;
        }
    }

    public class DiskBytesRead : AmountMetric
    {
        protected static String staticName = "DiskBytesRead";
        protected static String staticDescription = "The number of bytes read in a disk read operation";

        public DiskBytesRead()
        {
            base.name = staticName;
            base.description = staticDescription;
        }
    }

    public class MessageSize : AmountMetric
    {
        protected static String staticName = "MessageSize";
        protected static String staticDescription = "The size of a received message in bytes";

        public MessageSize()
        {
            base.name = staticName;
            base.description = staticDescription;
        }
    }

    class AvailableMemory : StatusMetric
    {
        protected static String staticName = "AvailableMemory";
        protected static String staticDescription = "The amount of free memory in the system in bytes";

        public AvailableMemory()
        {
            base.name = staticName;
            base.description = staticDescription;
        }
    }

    class ActiveWorkerThreads : StatusMetric
    {
        protected static String staticName = "ActiveWorkerThreads";
        protected static String staticDescription = "The number of active worker threads";

        public ActiveWorkerThreads()
        {
            base.name = staticName;
            base.description = staticDescription;
        }
    }

    class DiskReadTime : IntervalMetric
    {
        protected static String staticName = "DiskReadTime";
        protected static String staticDescription = "The time taken to perform a read operation from disk";

        public DiskReadTime()
        {
            base.name = staticName;
            base.description = staticDescription;
        }
    }

    class MessageReceiveTime : IntervalMetric
    {
        protected static String staticName = "MessageReceiveTime";
        protected static String staticDescription = "The time taken to received a message";

        public MessageReceiveTime()
        {
            base.name = staticName;
            base.description = staticDescription;
        }
    }
}
