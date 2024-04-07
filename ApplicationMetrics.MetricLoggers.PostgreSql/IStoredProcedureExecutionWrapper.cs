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
using Npgsql;

namespace ApplicationMetrics.MetricLoggers.PostgreSql
{
    /// <summary>
    /// A wrapper interface around methods which execute stored procedures in PostgreSQL, allowing those methods to be mocked in unit tests.
    /// </summary>
    public interface IStoredProcedureExecutionWrapper
    {
        /// <summary>
        /// Executes a stored procedure which does not return a result set.
        /// </summary>
        /// <param name="procedureName">The name of the stored procedure.</param>
        /// <param name="parameters">The parameters to pass to the stored procedure.</param>
        void Execute(String procedureName, IList<NpgsqlParameter> parameters);
    }
}
