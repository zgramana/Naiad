/*
 * Naiad ver. 0.6
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;

namespace Microsoft.Research.Naiad.Dataflow.BatchEntry
{
    internal class IngressVertex<R, T> : Vertex<T>
        where T : Time<T>
    {
        private readonly VertexOutputBufferForInterestingTime<T, R, BatchIn<T>> Output;

        public void MessageReceived(Message<R, T> message)
        {
            var output = this.Output.GetBufferForInterestingTime(new BatchIn<T>(message.time, 0));
            for (int i = 0; i < message.length; i++)
                output.Send(message.payload[i]);
        }

        public override string ToString()
        {
            return "BatchIngress";
        }

        internal static Stream<R, BatchIn<T>> NewStage(Stream<R, T> input, string name)
        {
            var stage = new Stage<IngressVertex<R, T>, T>(input.ForStage.InternalComputation, Stage.OperatorType.Ingress,
                (i, v) => new IngressVertex<R, T>(i, v), name);
            stage.SetCheckpointType(CheckpointType.Stateless);

            stage.NewInput(input, vertex => new ActionReceiver<R, T>(vertex, m => vertex.MessageReceived(m)), input.PartitionedBy);

            return stage.NewSurprisingTimeTypeOutput(vertex => vertex.Output, input.PartitionedBy, t => new BatchIn<T>(t, 0));
        }

        internal IngressVertex(int index, Stage<T> stage)
            : base(index, stage)
        {
            Output = new VertexOutputBufferForInterestingTime<T, R, BatchIn<T>>(this);
        }
    }

    internal class EgressVertex<R, T> : Vertex<BatchIn<T>>
        where T : Time<T>
    {
        private readonly VertexOutputBufferForInterestingTime<BatchIn<T>, R, T> outputs;

        public void OnReceive(Message<R, BatchIn<T>> message)
        {
            var output = this.outputs.GetBufferForInterestingTime(message.time.outerTime);
            for (int i = 0; i < message.length; i++)
                output.Send(message.payload[i]);
        }

        public override string ToString()
        {
            return "BatchEgress";
        }

        internal static Stream<R, T> NewStage(Stream<R, BatchIn<T>> input, string name)
        {
            var stage = new Stage<EgressVertex<R, T>, BatchIn<T>>(
                input.ForStage.InternalComputation, Stage.OperatorType.Egress,
                (i, v) => new EgressVertex<R, T>(i, v), name);
            stage.SetCheckpointType(CheckpointType.Stateless);

            stage.NewInput(input, vertex => new ActionReceiver<R, BatchIn<T>>(vertex, m => vertex.OnReceive(m)), input.PartitionedBy);

            return stage.NewSurprisingTimeTypeOutput<R, T>(vertex => vertex.outputs, input.PartitionedBy, t => t.outerTime);
        }

        internal EgressVertex(int index, Stage<BatchIn<T>> stage)
            : base(index, stage)
        {
            outputs = new VertexOutputBufferForInterestingTime<BatchIn<T>, R, T>(this);
        }
    }

    /// <summary>
    /// Represents a Naiad batch context
    /// </summary>
    /// <typeparam name="TTime">time type</typeparam>
    public class BatchContext<TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Introduces a stream into the batch context from outside
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="stream">stream</param>
        /// <param name="name">stream name</param>
        /// <returns>the same stream with an addition time coordinate</returns>
        public Stream<TRecord, BatchIn<TTime>> EnterBatch<TRecord>(Stream<TRecord, TTime> stream, string name = "EnterBatch")
        {
            return IngressVertex<TRecord, TTime>.NewStage(stream, name);
        }

        /// <summary>
        /// Extracts a stream from a batch context
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="stream">the stream</param>
        /// <param name="name">stream name</param>
        /// <returns>A stream containing records in the corresponding batch</returns>
        public Stream<TRecord, TTime> ExitBatch<TRecord>(Stream<TRecord, BatchIn<TTime>> stream, string name)
        {
            return EgressVertex<TRecord, TTime>.NewStage(stream, name);
        }
    }

    /// <summary>
    /// Extension methods for creating batch entry vertices
    /// </summary>
    public static class BatchedEntryExtensionMethods
    {
        /// <summary>
        /// Create a new batched entry subgraph
        /// </summary>
        /// <typeparam name="S">type of records exiting the subgraph</typeparam>
        /// <typeparam name="T">time type of records after exiting</typeparam>
        /// <param name="computation">computation graph</param>
        /// <param name="entryComputation">function within the subgraph</param>
        /// <param name="name">exit stage name</param>
        /// <returns>the function after exiting the subbatches</returns>
        public static Stream<S, T> BatchedEntry<S, T>(this Computation computation, Func<BatchContext<T>, Stream<S, BatchIn<T>>> entryComputation, string name = "ExitBatch")
            where T : Time<T>
        {
            var helper = new BatchContext<T>();

            var batchedOutput = entryComputation(helper);

            return helper.ExitBatch(batchedOutput, name);
        }
    }
}
