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
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.BatchEntry;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.FaultToleranceManager;

using Microsoft.Research.Naiad.Examples.DifferentialDataflow;

namespace FaultToleranceExamples
{
    public static class ExtensionMethods
    {
        public static Stream<S, T> Compose<R, S, T>(this Stream<R, T> input,
            Computation computation, Placement placement,
            Func<Stream<R,T>, Stream<S,T>> function)
            where T : Time<T>
        {
            Stream<S, T> output = null;
            using (var ov = computation.WithPlacement(placement))
            {
                output = function(input);
            }
            return output;
        }

        public static Collection<S, T> Compose<R, S, T>(this Collection<R, T> input,
            Computation computation, Placement placement,
            Func<Collection<R, T>, Collection<S, T>> function)
            where T : Time<T>
            where R : IEquatable<R>
            where S : IEquatable<S>
        {
            Collection<S, T> output = null;
            using (var ov = computation.WithPlacement(placement))
            {
                output = function(input);
            }
            return output;
        }

        public static Pair<Stream<Program.FastPipeline.Record, TInner>, Stream<bool, TInner>>
            StaggeredJoin<TInput1, TInput2, TKey, TInner, TOuter>(
            this Stream<TInput1, TInner> stream1,
            Stream<TInput2, TInner> stream2,
            Stream<Pair<int, Pair<long, long>>, TInner> stream2Window,
            Func<TInput1, TKey> keySelector1, Func<TInput2, TKey> keySelector2,
            Func<TInput1, TInput2, Pair<long, long>, Program.FastPipeline.Record> resultSelector,
            Func<TInner, TOuter> timeSelector, Func<TOuter, TInner> maxBatchTimeSelector,
            string name)
            where TInput2 : Program.IRecord
            where TInner : Time<TInner>
            where TOuter : Time<TOuter>
        {
            return Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>.MakeStage(
                stream1, stream2, stream2Window, keySelector1, keySelector2, resultSelector, timeSelector, maxBatchTimeSelector, name);
        }

        public static Stream<R, T> Prepend<R, T>(this Stream<R, T> stream, Expression<Func<R,int>> partitionBy)
            where R : Program.IRecord
            where T : Time<T>
        {
            return stream.NewUnaryStage((i, s) => new Program.FastPipeline.PrependVertex<R, T>(i, s),
                partitionBy, partitionBy, "Prepend")
                .SetCheckpointType(CheckpointType.StatelessLogEphemeral)
                .SetCheckpointPolicy(v => new CheckpointWithoutPersistence());
        }

        public static void PartitionedActionStage<R>(this Stream<Pair<int,R>, Epoch> stream, Action<R> action)
        {
            Program.PartitionedActionVertex<R>.PartitionedActionStage(stream, action);
        }

        public static void PartitionedActionStage(this Stream<int, Epoch> stream, Action action)
        {
            Program.PartitionedActionVertex.PartitionedActionStage(stream, action);
        }
    }

    public class Program
    {
        private string accountName = null;
        private string accountKey = null;
        private string containerName = "checkpoint";

        public interface IRecord
        {
            long EntryTicks { get; set; }
        }

        public class SlowPipeline
        {
            public int baseProc;
            public int range;
            public SubBatchDataSource<HTRecord, Epoch> source;

            public struct Record : IRecord, IEquatable<Record>
            {
                public int key;
                public long count;
                public long entryTicks;
                public long EntryTicks { get { return this.entryTicks; } set { this.entryTicks = value; } }

                public Record(HTRecord large)
                {
                    this.key = large.key;
                    this.count = -1;
                    this.entryTicks = large.entryTicks;
                }

                public bool Equals(Record other)
                {
                    return key == other.key && EntryTicks == other.EntryTicks && count == other.count;
                }

                public override string ToString()
                {
                    return key + " " + count + " " + EntryTicks;
                }
            }

            public Stream<Record, BatchIn<Epoch>> Reduce(Stream<HTRecord, BatchIn<Epoch>> input)
            {
                var smaller = input.Select(r => new Record(r)).SetCheckpointPolicy(i => new CheckpointEagerly());
                var count = smaller.Select(r => r.key).SetCheckpointPolicy(i => new CheckpointEagerly())
                    .Count(i => new CheckpointEagerly());

                var consumable = smaller
                    .Join(count, r => r.key, c => c.First, (r, c) => { r.count = c.Second; return r; }).SetCheckpointPolicy(i => new CheckpointEagerly());
                var reduced = consumable.SetCheckpointType(CheckpointType.StatelessLogAll).SetCheckpointPolicy(i => new CheckpointEagerly());

                this.reduceStage = reduced.ForStage.StageId;
                return reduced;
            }

            public Stream<Pair<long,long>, Epoch> TimeWindow(Stream<Record, Epoch> input, int workerCount)
            {
                var parallelMin = input
                    .Where(r => r.EntryTicks > 0).SetCheckpointPolicy(i => new CheckpointEagerly())
                    .Min(r => r.key.GetHashCode() % workerCount, r => r.EntryTicks, i => new CheckpointEagerly());
                var min = parallelMin.Min(r => true, r => r.Second, i => new CheckpointEagerly());

                var parallelMax = input
                    .Where(r => r.EntryTicks > 0).SetCheckpointPolicy(i => new CheckpointEagerly())
                    .Max(r => r.key.GetHashCode() % workerCount, r => r.EntryTicks, i => new CheckpointEagerly());
                var max = parallelMax.Max(r => true, r => r.Second, i => new CheckpointEagerly());

                var consumable = min
                    .Join(max, mi => mi.First, ma => ma.First, (mi, ma) => mi.Second.PairWith(ma.Second)).SetCheckpointPolicy(i => new CheckpointEagerly());
                var window = consumable.SetCheckpointType(CheckpointType.StatelessLogAll).SetCheckpointPolicy(i => new CheckpointEagerly());

                return window;
            }

            public Stream<Record, Epoch> Compute(Stream<Record, Epoch> input)
            {
                return input;
            }

            public int reduceStage;
            public IEnumerable<int> ToMonitor
            {
                get { return new int[] { reduceStage }; }
            }

            public Pair<Stream<Record, Epoch>, Stream<Pair<long, long>, Epoch>> Make(Computation computation)
            {
                this.source = new SubBatchDataSource<HTRecord, Epoch>();

                Placement placement =
                    new Placement.ProcessRange(Enumerable.Range(this.baseProc, this.range),
                        Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));

                Stream<Record, Epoch> computed;
                Stream<Pair<long, long>, Epoch> window;

                using (var p = computation.WithPlacement(placement))
                {
                    var reduced = computation.BatchedEntry<Record, Epoch>(c =>
                            {
                                var input = computation.NewInput(this.source)
                                    .SetCheckpointType(CheckpointType.CachingInput)
                                    .SetCheckpointPolicy(s => new CheckpointEagerly());
                                return this.Reduce(input);
                            }, "ExitSlowBatch");

                    computed = this.Compute(reduced);

                    window = this.TimeWindow(reduced, placement.Count);
                }

                return computed.PairWith(window);
            }

            public SlowPipeline(int baseProc, int range)
            {
                this.baseProc = baseProc;
                this.range = range;
            }
        }

        public class FastPipeline
        {
            private Configuration config;
            private StreamWriter checkpointLog = null;
            internal StreamWriter CheckpointLog
            {
                get
                {
                    if (checkpointLog == null)
                    {
                        string fileName = String.Format("fastPipe.{0:D3}.log", this.config.ProcessID);
                        checkpointLog = this.config.LogStreamFactory(fileName).Log;
                    }
                    return checkpointLog;
                }
            }

            public void WriteLog(string entry, params object[] args)
            {
                var log = this.CheckpointLog;
                lock (log)
                {
                    log.WriteLine(entry, args);
                }
            }

            public int queryProc;
            public int baseProc;
            public int range;
            public int workerCount;

            public class PrependVertex<R, T> : UnaryVertex<R, R, T> where T : Time<T> where R : IRecord
            {
                private readonly HashSet<T> seenAny = new HashSet<T>();

                public override void OnReceive(Message<R, T> message)
                {
                    var output = this.Output.GetBufferForTime(message.time);
                    if (!seenAny.Contains(message.time))
                    {
                        //Console.WriteLine(this.VertexId + " prepend " + message.time);
                        this.NotifyAt(message.time);
                        this.seenAny.Add(message.time);

                        R r = default(R);
                        r.EntryTicks = -1;
                        output.Send(r);
                    }

                    for (int i = 0; i < message.length; i++)
                    {
                        output.Send(message.payload[i]);
                    }
                }

                public override void OnNotify(T time)
                {
                    var output = this.Output.GetBufferForTime(time);
                    R r = default(R);
                    r.EntryTicks = -2;
                    output.Send(r);

                    this.seenAny.Remove(time);
                }

                public PrependVertex(int index, Stage<T> stage) : base(index, stage)
                {
                }
            }

            public class StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter> : Vertex<TInner>
                where TInput2 : IRecord
                where TInner : Time<TInner>
                where TOuter : Time<TOuter>
            {
                private readonly Dictionary<TOuter, Dictionary<TKey, List<TInput2>>> partialValues = new Dictionary<TOuter, Dictionary<TKey, List<TInput2>>>();
                private readonly Dictionary<TOuter, Dictionary<TKey, List<TInput2>>> values = new Dictionary<TOuter, Dictionary<TKey, List<TInput2>>>();
                private readonly Dictionary<TOuter, Pair<long, long>> windows = new Dictionary<TOuter, Pair<long, long>>();
                private readonly HashSet<TOuter> announcedTimes = new HashSet<TOuter>();

                private readonly Func<TInput1, TKey> keySelector1;
                private readonly Func<TInput2, TKey> keySelector2;
                private readonly Func<TInput1, TInput2, Pair<long, long>, Record> resultSelector;
                private readonly Func<TInner, TOuter> timeSelector;
                private readonly Func<TOuter, TInner> maxBatchTimeSelector;

                private VertexOutputBuffer<Record, TInner> Output;
                private VertexOutputBuffer<bool, TInner> readyOutput;

                protected override bool CanRollBackPreservingState(Pointstamp[] frontier)
                {
                    return true;
                }

                public override void RollBackPreservingState(Pointstamp[] frontier, ICheckpoint<TInner> lastFullCheckpoint, ICheckpoint<TInner> lastIncrementalCheckpoint)
                {
                    base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);
                }

                public void OnReceive1(Message<TInput1, TInner> message)
                {
                    //Console.WriteLine(this.Stage.Name + " Receive1 " + message.time);
                    TOuter outer = timeSelector(message.time);

                    Dictionary<TKey, List<TInput2>> currentValues = this.values[outer];
                    Pair<long, long> window = this.windows[outer];

                    var output = this.Output.GetBufferForTime(message.time);

                    for (int i = 0; i < message.length; i++)
                    {
                        var key = keySelector1(message.payload[i]);

                        List<TInput2> currentEntry;
                        if (currentValues.TryGetValue(key, out currentEntry))
                        {
                            foreach (var match in currentEntry)
                                output.Send(resultSelector(message.payload[i], match, window));
                        }
                        else
                        {
                            Console.WriteLine(this.Stage.Name + "." + this.VertexId + " no matches for " + message.time + " " + key);
                        }
                    }
                }

                public void OnReceive2(Message<TInput2, TInner> message)
                {
                    //Console.WriteLine(this.Stage.Name + "." + this.VertexId + " Receive2 " + message.time);
                    TOuter outerTime = timeSelector(message.time);

                    int baseRecord = 0;

                    Dictionary<TKey, List<TInput2>> currentValues;

                    if (message.payload[0].EntryTicks == -1)
                    {
                        //Console.WriteLine("Notifying at " + maxBatchTimeSelector(outerTime) + " for " + message.time);
                        this.NotifyAt(maxBatchTimeSelector(outerTime));

                        ++baseRecord;
                        if (this.partialValues.ContainsKey(outerTime))
                        {
                            Console.WriteLine(this.Stage.Name + "." + this.VertexId + " Replacing partial values for " + outerTime);
                        }
                        this.partialValues[outerTime] = new Dictionary<TKey, List<TInput2>>();
                    }
                    else if (!this.partialValues.ContainsKey(outerTime))
                    {
                        throw new ApplicationException(this.Stage.Name + " not accumulating partial values for " + outerTime);
                    }

                    currentValues = this.partialValues[outerTime];

                    for (int i = baseRecord; i < message.length; i++)
                    {
                        if (message.payload[i].EntryTicks == -2)
                        {
                            if (this.values.ContainsKey(outerTime))
                            {
                                Console.WriteLine(this.Stage.Name + "." + this.VertexId + " replacing values for " + outerTime);
                            }
                            this.values[outerTime] = this.partialValues[outerTime];
                            this.partialValues.Remove(outerTime);
                            if (this.windows.ContainsKey(outerTime) && !this.announcedTimes.Contains(outerTime))
                            {
                                this.announcedTimes.Add(outerTime);
                                var ready = this.readyOutput.GetBufferForTime(message.time);
                                ready.Send(true);
                            }
                            Console.WriteLine(this.Stage.Name + "." + this.VertexId + " R2 " + outerTime + ": " + currentValues.Select(k => k.Value.Count).Sum());
                            //if (this.Stage.Name == "SlowJoin")
                            //{
                            //foreach (var k in currentValues.Keys)
                            //{
                            //    Console.WriteLine(this.Stage.Name + " R2 " + k + ": " + currentValues[k].Count);
                                //foreach (var v in currentValues[k])
                                //{
                                //    Console.WriteLine("  " + this.Stage.Name + " R2 " + v + " ");
                                //}
                            //}
                            //}
                        }
                        else
                        {
                            var key = keySelector2(message.payload[i]);

                            List<TInput2> currentEntry;
                            if (!currentValues.TryGetValue(key, out currentEntry))
                            {
                                currentEntry = new List<TInput2>();
                                currentValues[key] = currentEntry;
                            }

                            currentEntry.Add(message.payload[i]);
                        }
                    }
                }

                public void OnReceive3(Message<Pair<int, Pair<long, long>>, TInner> message)
                {
                    //Console.WriteLine(this.Stage.Name + " receive window " + message.time + message.payload[0]);
                    TOuter outerTime = timeSelector(message.time);

                    if (this.windows.ContainsKey(outerTime))
                    {
                        Console.WriteLine(this.Stage.Name + " replacing window for " + outerTime);
                    }
                    this.windows[outerTime] = message.payload[0].Second;
                    if (this.values.ContainsKey(outerTime) && !this.announcedTimes.Contains(outerTime))
                    {
                        this.announcedTimes.Add(outerTime);
                        var ready = this.readyOutput.GetBufferForTime(message.time);
                        ready.Send(true);
                    }
                }

                public override void OnNotify(TInner time)
                {
                    TOuter outerTime = timeSelector(time);

                    if (time.Equals(this.maxBatchTimeSelector(outerTime)))
                    {
                        //Console.WriteLine("Removing " + outerTime + " for " + time);

                        this.values.Remove(outerTime);
                        this.windows.Remove(outerTime);
                        this.announcedTimes.Remove(outerTime);

                        if (this.partialValues.ContainsKey(outerTime))
                        {
                            throw new ApplicationException(this.Stage.Name + " Leftover partial values for " + outerTime);
                        }
                    }
                }

                public StaggeredJoinVertex(int index, Stage<TInner> stage,
                    Func<TInput1, TKey> key1, Func<TInput2, TKey> key2,
                    Func<TInput1, TInput2, Pair<long, long>, Record> result,
                    Func<TInner, TOuter> timeSelector, Func<TOuter, TInner> maxBatchTimeSelector)
                    : base(index, stage)
                {
                    this.values = new Dictionary<TOuter, Dictionary<TKey, List<TInput2>>>();
                    this.keySelector1 = key1;
                    this.keySelector2 = key2;
                    this.resultSelector = result;
                    this.timeSelector = timeSelector;
                    this.maxBatchTimeSelector = maxBatchTimeSelector;

                    this.Output = new VertexOutputBuffer<Record, TInner>(this);
                    this.readyOutput = new VertexOutputBuffer<bool, TInner>(this);
                }

                public static Pair<Stream<Record, TInner>, Stream<bool, TInner>> MakeStage(
                    Stream<TInput1, TInner> stream1, Stream<TInput2, TInner> stream2, Stream<Pair<int, Pair<long, long>>, TInner> stream3,
                    Func<TInput1, TKey> keySelector1, Func<TInput2, TKey> keySelector2,
                    Func<TInput1, TInput2, Pair<long, long>, Record> resultSelector,
                    Func<TInner, TOuter> timeSelector, Func<TOuter, TInner> maxBatchTimeSelector,
                    string name)
                {
                    var stage = Foundry.NewStage<Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>, TInner>(
                        stream1.Context,
                        (i, s) => new Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>
                            (i, s, keySelector1, keySelector2, resultSelector, timeSelector, maxBatchTimeSelector),
                            name);
                    stage.SetCheckpointType(CheckpointType.StatelessLogEphemeral);
                    stage.SetCheckpointPolicy(v => new CheckpointWithoutPersistence());

                    var input1 = stage.NewInput(stream1, (message, vertex) => vertex.OnReceive1(message), x => keySelector1(x).GetHashCode());
                    var input2 = stage.NewInput(stream2, (message, vertex) => vertex.OnReceive2(message), null);
                    var input3 = stage.NewInput(stream3, (message, vertex) => vertex.OnReceive3(message), w => w.First);

                    var output = stage.NewOutput(vertex => vertex.Output);
                    var readyOutput = stage.NewOutput(vertex => vertex.readyOutput);

                    return output.PairWith(readyOutput);
                }
            }

            public class ExitVertex : UnaryVertex<Record, Record, BatchIn<BatchIn<Epoch>>>
            {
                private readonly FastPipeline parent;

                public override void OnReceive(Message<Record, BatchIn<BatchIn<Epoch>>> message)
                {
                    parent.HoldOutputs(message);
                }

                private ExitVertex(int index, Stage<BatchIn<BatchIn<Epoch>>> stage, FastPipeline parent)
                    : base(index, stage)
                {
                    this.parent = parent;
                }

                public static Stream<Record, BatchIn<BatchIn<Epoch>>> ExitStage(
                    Stream<Record, BatchIn<BatchIn<Epoch>>> stream,
                    FastPipeline parent)
                {
                    return stream.NewUnaryStage<Record, Record, BatchIn<BatchIn<Epoch>>>(
                        (i,s) => new ExitVertex(i, s, parent), null, null, "ExitFastPipeline")
                        .SetCheckpointType(CheckpointType.StatelessLogEphemeral)
                        .SetCheckpointPolicy(v => new CheckpointWithoutPersistence());
                }
            }

            public class JoinReadyVertex : SinkVertex<bool, BatchIn<BatchIn<Epoch>>>
            {
                private readonly Action<BatchIn<BatchIn<Epoch>>> ready;
                private readonly Dictionary<BatchIn<BatchIn<Epoch>>, int> remaining;
                private int numSenders;

                public override void OnReceive(Message<bool, BatchIn<BatchIn<Epoch>>> message)
                {
                    if (!remaining.ContainsKey(message.time))
                    {
                        remaining[message.time] = this.numSenders;
                        this.NotifyAt(message.time);
                    }

                    for (int i = 0; i < message.payload.Length; ++i)
                    {
                        --remaining[message.time];
                        if (remaining[message.time] == 0)
                        {
                            this.ready(message.time);
                        }
                    }
                }

                public override void OnNotify(BatchIn<BatchIn<Epoch>> time)
                {
                    this.remaining.Remove(time);
                }

                private JoinReadyVertex(int index, Stage<BatchIn<BatchIn<Epoch>>> stage, int numSenders, Action<BatchIn<BatchIn<Epoch>>> ready)
                    : base(index, stage)
                {
                    this.ready = ready;
                    this.numSenders = numSenders;
                    this.remaining = new Dictionary<BatchIn<BatchIn<Epoch>>,int>();
                }

                public static void JoinReadyStage(
                    Stream<bool, BatchIn<BatchIn<Epoch>>> stream,
                    int num, Action<BatchIn<BatchIn<Epoch>>> ready, string name)
                {
                    var stage = stream.NewSinkStage<bool, BatchIn<BatchIn<Epoch>>>(
                        (i, s) => new JoinReadyVertex(i, s, num, ready), null, name);
                    stage.SetCheckpointType(CheckpointType.StatelessLogEphemeral);
                    stage.SetCheckpointPolicy(v => new CheckpointWithoutPersistence());
                }
            }

            public struct Record : IEquatable<Record>
            {
                public long startMs;
                public Pair<long, long> slowWindow;
                public Pair<long, long> ccWindow;
                public int slowJoinKey;
                public int ccJoinKey;

                public bool Equals(Record other)
                {
                    return startMs == other.startMs &&
                        slowWindow.Equals(other.slowWindow) &&
                        ccWindow.Equals(other.ccWindow) &&
                        slowJoinKey == other.slowJoinKey &&
                        ccJoinKey == other.ccJoinKey;
                }
            }

            private HashSet<Epoch> slowDataReady = new HashSet<Epoch>();
            private Epoch? slowDataStable;
            private HashSet<BatchIn<Epoch>> ccDataReady = new HashSet<BatchIn<Epoch>>();
            private BatchIn<Epoch>? ccDataStable;
            private BatchIn<Epoch>? fastTime;

            public void AcceptSlowDataReady(Epoch slowTime)
            {
                lock (this)
                {
                    this.slowDataReady.Add(slowTime);
                    Console.WriteLine("Fast got slow data " + slowTime);
                }

                this.ConsiderFastBatches();
            }

            public void AcceptSlowDataStable(Epoch slowTime)
            {
                lock (this)
                {
                    if (!this.slowDataStable.HasValue || !slowTime.LessThan(this.slowDataStable.Value))
                    {
                        Console.WriteLine("Fast got slow stable " + slowTime);
                        this.slowDataStable = slowTime;
                    }
                }

                this.ConsiderFastBatches();
            }

            public void AcceptCCDataReady(BatchIn<Epoch> ccTime)
            {
                Console.WriteLine("Fast got CC data " + ccTime);

                if (ccTime.batch == int.MaxValue)
                {
                    return;
                }

                lock (this)
                {
                    this.ccDataReady.Add(ccTime);
                }

                this.ConsiderFastBatches();
            }

            public void AcceptCCDataStable(BatchIn<Epoch> ccTime)
            {
                Console.WriteLine("Fast got CC stable " + ccTime);

                lock (this)
                {
                    if (!this.ccDataStable.HasValue || !ccTime.LessThan(this.ccDataStable.Value))
                    {
                        this.ccDataStable = ccTime;
                    }
                }

                this.ConsiderFastBatches();
            }

            private void ConsiderFastBatches()
            {
                bool start = false;

                lock (this)
                {
                    if (this.slowDataStable.HasValue && this.ccDataStable.HasValue)
                    {
                        var goodSlowData = this.slowDataReady.Where(x => x.LessThan(this.slowDataStable.Value)).ToArray();
                        if (goodSlowData.Length == 0)
                        {
                            return;
                        }
                        var goodCCData = this.ccDataReady.Where(x => x.LessThan(this.ccDataStable.Value)).ToArray();
                        if (goodCCData.Length == 0)
                        {
                            return;
                        }

                        Epoch slowTime = goodSlowData.Max();
                        BatchIn<Epoch> ccTime = goodCCData.Max();
                        BatchIn<Epoch> newFastTime;

                        if (ccTime.outerTime.epoch > slowTime.epoch)
                        {
                            newFastTime = new BatchIn<Epoch>(slowTime, int.MaxValue);
                        }
                        else
                        {
                            newFastTime = ccTime;
                        }

                        start = !this.fastTime.HasValue;

                        if (!this.fastTime.HasValue || !newFastTime.LessThan(this.fastTime.Value))
                        {
                            this.fastTime = newFastTime;
                            foreach (var t in goodSlowData)
                            {
                                this.slowDataReady.Remove(t);
                            }
                            this.slowDataReady.Add(slowTime);
                            foreach (var t in goodCCData)
                            {
                                this.ccDataReady.Remove(t);
                            }
                            this.ccDataReady.Add(ccTime);

                            Console.WriteLine("Setting new fast time " + this.fastTime.Value);
                            this.dataSource.StartOuterBatch(this.fastTime.Value);
                        }
                    }
                }

                if (start)
                {
                    var thread = new System.Threading.Thread(new System.Threading.ThreadStart(() => FeedThread()));
                    thread.Start();
                }
            }

            private Random random = new Random();

            private IEnumerable<Record> MakeBatch(int count)
            {
                long ms = program.computation.TicksSinceStartup / TimeSpan.TicksPerMillisecond;
                for (int i=0; i<count; ++i)
                {
                    yield return new Record
                    {
                        startMs = ms,
                        slowWindow = (-1L).PairWith(-1L),
                        ccWindow = (-1L).PairWith(-1L),
                        slowJoinKey = random.Next(Program.numberOfKeys),
                        ccJoinKey = random.Next(Program.numberOfKeys),
                    };
                }

                for (int i=0; i<this.workerCount; ++i)
                {
                    yield return new Record
                    {
                        startMs = ms,
                        slowWindow = (-1L).PairWith(-1L),
                        ccWindow = (-1L).PairWith(-1L),
                        slowJoinKey = i,
                        ccJoinKey = i,
                    };
                }
            }

            private void FeedThread()
            {
                while (true)
                {
                    Console.WriteLine("Sending fast batch " + this.dataSource.NextTime());

                    this.dataSource.OnNext(this.MakeBatch(Program.fastBatchSize));

                    Thread.Sleep(Program.fastSleepTime);
                }
            }

            private Dictionary<BatchIn<BatchIn<Epoch>>, List<Record>>
                bufferedOutputs = new Dictionary<BatchIn<BatchIn<Epoch>>, List<Record>>();

            private Pointstamp holdTime = new Pointstamp();

            private Pointstamp ToPointstamp(BatchIn<BatchIn<Epoch>> time)
            {
                Pointstamp stamp = new Pointstamp();
                stamp.Location = this.resultStage;
                stamp.Timestamp.Length = 3;
                stamp.Timestamp.a = time.outerTime.outerTime.epoch;
                stamp.Timestamp.b = time.outerTime.batch;
                stamp.Timestamp.c = time.batch;
                return stamp;
            }

            private void HoldOutputs(Message<Record, BatchIn<BatchIn<Epoch>>> message)
            {
                lock (this)
                {
                    Pointstamp time = this.ToPointstamp(message.time);
                    if (holdTime.Location != 0 && FTFrontier.IsLessThanOrEqualTo(time, holdTime))
                    {
                        throw new ApplicationException("Behind the times");
                    }

                    List<Record> buffer;
                    if (!this.bufferedOutputs.TryGetValue(message.time, out buffer))
                    {
                        buffer = new List<Record>();
                        this.bufferedOutputs.Add(message.time, buffer);
                        Console.WriteLine("Holding records for " + message.time);
                    }
                    for (int i = 0; i < message.length; ++i)
                    {
                        buffer.Add(message.payload[i]);
                    }
                }
            }

            public void ReleaseOutputs(Pointstamp time)
            {
                long doneMs = program.computation.TicksSinceStartup / TimeSpan.TicksPerMillisecond;

                Console.WriteLine("Releasing records up to " + time);

                List<Pair<BatchIn<BatchIn<Epoch>>, Record>> released = new List<Pair<BatchIn<BatchIn<Epoch>>, Record>>();

                lock (this)
                {
                    if (this.holdTime.Location == 0 || !FTFrontier.IsLessThanOrEqualTo(time, this.holdTime))
                    {
                        this.holdTime = time;
                        var readyTimes = this.bufferedOutputs.Keys
                            .Where(t => FTFrontier.IsLessThanOrEqualTo(this.ToPointstamp(t), this.holdTime))
                            .ToArray();
                        foreach (var ready in readyTimes)
                        {
                            Console.WriteLine("READY " + ready);
                            foreach (var record in this.bufferedOutputs[ready])
                            {
                                released.Add(ready.PairWith(record));
                            }
                            this.bufferedOutputs.Remove(ready);
                        }
                    }
                }

                var totalTicks = this.computation.Controller.Stopwatch.ElapsedTicks;
                var totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;

                foreach (var record in released.Take(1))
                {
                    if (record.Second.startMs == -1)
                    {
                        this.WriteLog("-1 -1 -1 {0:D11}", totalMicroSeconds);
                        Console.WriteLine("-1");
                    }
                    else
                    {
                        long slowBatchMs = -1;
                        if (record.Second.slowWindow.First >= 0)
                        {
                            slowBatchMs = record.Second.slowWindow.Second / TimeSpan.TicksPerMillisecond;
                        }
                        long ccBatchMs = -1;
                        if (record.Second.ccWindow.First >= 0)
                        {
                            ccBatchMs = record.Second.ccWindow.Second / TimeSpan.TicksPerMillisecond;
                        }

                        long latency = doneMs - record.Second.startMs;
                        long slowStaleness = (slowBatchMs < 0) ? -2 : doneMs - slowBatchMs;
                        long ccStaleness = (ccBatchMs < 0) ? -2 : doneMs - ccBatchMs;
                        this.WriteLog("{0:D11} {1:D11} {2:D11} {3:D11}", latency, slowStaleness, ccStaleness, totalMicroSeconds);
                        Console.WriteLine("{0:D11} {1:D11} {2:D11}", latency, slowStaleness, ccStaleness);
                    }
                }
            }

            private Computation computation;
            private SubBatchDataSource<Record, BatchIn<Epoch>> dataSource;
            public int slowStage;
            public int ccStage;
            public int resultStage;

            public IEnumerable<int> ToMonitor
            {
                get { return new int[] { this.slowStage, this.ccStage, this.resultStage }; }
            }

            public void Make(Computation computation,
                Stream<SlowPipeline.Record, BatchIn<Epoch>> slowOutput,
                Stream<Pair<long, long>, BatchIn<Epoch>> slowTimeWindow,
                Placement slowPlacement,
                Collection<CCPipeline.Record, BatchIn<Epoch>> ccOutput,
                Stream<Pair<long, long>, BatchIn<Epoch>> ccTimeWindow,
                Placement ccPlacement)
            {
                this.computation = computation;

                this.dataSource = new SubBatchDataSource<Record, BatchIn<Epoch>>();

                Placement fastPlacement = new Placement.ProcessRange(Enumerable.Range(this.baseProc, this.range), Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));
                this.workerCount = this.range * computation.Controller.Configuration.WorkerCount;

                Placement senderPlacement = new Placement.ProcessRange(Enumerable.Range(this.queryProc, 1), Enumerable.Range(0, 1));

                // send the queries from a single worker
                    using (var sender = computation.WithPlacement(senderPlacement))
                    {
                        var queries = computation.NewInput(dataSource, "FastQueries").SetCheckpointType(CheckpointType.CachingInput);

                        // keep the single placement for the batchedentry since that means the exit vertex will be routed via that same worker
                        var output = computation.BatchedEntry<Record, BatchIn<Epoch>>(ic =>
                            {
                                Stream<SlowPipeline.Record, BatchIn<BatchIn<Epoch>>> slowInternal;
                                Stream<Pair<int, Pair<long, long>>, BatchIn<BatchIn<Epoch>>> slowWindowInternal;
                                using (var prepareSlow = computation.WithPlacement(slowPlacement))
                                {
                                    slowInternal = ic
                                        .EnterBatch(slowOutput, "SlowDataEntry")
                                        .SetCheckpointPolicy(v => new CheckpointEagerly())
                                        .SetCheckpointType(CheckpointType.StatelessLogAll);
                                    var broadcastSlowWindow = slowTimeWindow
                                        .SelectMany(w => Enumerable.Range(0, fastPlacement.Count).Select(d => d.PairWith(w)));
                                    slowWindowInternal = ic
                                        .EnterBatch(broadcastSlowWindow, "SlowWindowEntry")
                                        .SetCheckpointPolicy(v => new CheckpointEagerly())
                                        .SetCheckpointType(CheckpointType.StatelessLogAll);
                                    var slowDone = slowInternal.Select(r => true).Concat(slowWindowInternal.Select(r => true));
                                    this.slowStage = slowDone.ForStage.StageId;
                                }

                                Stream<CCPipeline.Record, BatchIn<BatchIn<Epoch>>> ccInternal;
                                Stream<Pair<int, Pair<long, long>>, BatchIn<BatchIn<Epoch>>> ccWindowInternal;
                                using (var prepareCC = computation.WithPlacement(ccPlacement))
                                {
                                    var cc = ccOutput
                                        .ToStateless().Output
                                        .SelectMany(r => Enumerable.Repeat(r.record, (int)Math.Max(0, r.weight)));
                                    ccInternal = ic
                                        .EnterBatch(cc, "CCDataEntry")
                                        .SetCheckpointPolicy(v => new CheckpointEagerly())
                                        .SetCheckpointType(CheckpointType.StatelessLogAll);
                                    var broadcastCCWindow = ccTimeWindow
                                        .SelectMany(w => Enumerable.Range(0, fastPlacement.Count).Select(d => d.PairWith(w)));
                                    ccWindowInternal = ic
                                        .EnterBatch(broadcastCCWindow, "CCWindowEntry")
                                        .SetCheckpointPolicy(v => new CheckpointEagerly())
                                        .SetCheckpointType(CheckpointType.StatelessLogAll);
                                    var ccDone = ccInternal.Select(r => true).Concat(ccWindowInternal.Select(r => true));
                                    this.ccStage = ccDone.ForStage.StageId;
                                }

                                // do the computation using the pipeline placement
                                using (var computeProcs = computation.WithPlacement(fastPlacement))
                                {
                                    var slowForJoin = slowInternal.Prepend(r => r.key);
                                    var firstJoin = queries.SetCheckpointPolicy(i => new CheckpointWithoutPersistence())
                                        .StaggeredJoin(
                                            slowForJoin,
                                            slowWindowInternal,
                                            i => i.slowJoinKey, s => s.key, (i, s, w) => { i.slowWindow = w; return i; },
                                            t => t.outerTime.outerTime, t => new BatchIn<BatchIn<Epoch>>(new BatchIn<Epoch>(t, Int32.MaxValue-1), Int32.MaxValue-1),
                                            "SlowJoin");

                                    var ccForJoin = ccInternal.Prepend(r => r.key);
                                    var secondJoin = firstJoin.First
                                        .StaggeredJoin(
                                            ccForJoin,
                                            ccWindowInternal,
                                            i => i.ccJoinKey, c => c.key, (i, c, w) => { i.ccWindow = w; return i; },
                                            t => t.outerTime, t => new BatchIn<BatchIn<Epoch>>(t, Int32.MaxValue-1),
                                            "CCJoin");

                                    // now collect the ready signals at the sender vertex
                                    using (var readyProcs = computation.WithPlacement(senderPlacement))
                                    {
                                        JoinReadyVertex.JoinReadyStage(firstJoin.Second, fastPlacement.Count, t => this.AcceptSlowDataReady(t.outerTime.outerTime), "SlowReady");
                                        JoinReadyVertex.JoinReadyStage(secondJoin.Second, fastPlacement.Count, t => this.AcceptCCDataReady(t.outerTime), "CCReady");
                                        var exit = ExitVertex.ExitStage(secondJoin.First, this);
                                        this.resultStage = exit.ForStage.StageId;
                                        return exit;
                                    }
                                }

                            }, "FastPipeLineExitBatch")
                                .SetCheckpointType(CheckpointType.StatelessLogEphemeral)
                                .SetCheckpointPolicy(s => new CheckpointWithoutPersistence());
                    }
            }

            public FastPipeline(Configuration config, int queryProc, int baseProc, int range)
            {
                this.config = config;
                this.queryProc = queryProc;
                this.baseProc = baseProc;
                this.range = range;
            }
        }

        public class CCPipeline
        {
            public int baseProc;
            public int range;
            public SubBatchDataSource<HTRecord, BatchIn<Epoch>> source;

            public struct Record : IRecord, IEquatable<Record>
            {
                public int key;
                public int otherKey;
                public long entryTicks;
                public long EntryTicks { get { return this.entryTicks; } set { this.entryTicks = value; } }

                public Record(HTRecord large)
                {
                    this.key = large.key;
                    this.otherKey = large.otherKey;
                    this.entryTicks = large.entryTicks;
                }

                public bool Equals(Record other)
                {
                    return key == other.key && EntryTicks == other.EntryTicks && otherKey == other.otherKey;
                }

                public override string ToString()
                {
                    return key + " " + otherKey + " " + entryTicks;
                }
            }

            private Stream<Record, BatchIn<BatchIn<Epoch>>> Reduce(Stream<HTRecord, BatchIn<BatchIn<Epoch>>> input)
            {
                var smaller = input.Select(r => new Record(r)).SetCheckpointPolicy(i => new CheckpointEagerly());
                var consumable = smaller.PartitionBy(r => r.key).SetCheckpointPolicy(i => new CheckpointEagerly());
                var reduced = consumable.SetCheckpointType(CheckpointType.StatelessLogAll).SetCheckpointPolicy(i => new CheckpointEagerly());
                this.reduceStage = reduced.ForStage.StageId;
                return reduced;
            }

            public Stream<Pair<long, long>, BatchIn<Epoch>> TimeWindow(Stream<Record, BatchIn<Epoch>> input, int workerCount)
            {
                var parallelMin = input
                    .Where(r => r.entryTicks > 0).SetCheckpointPolicy(i => new CheckpointEagerly())
                    .Min(r => r.key.GetHashCode() % workerCount, r => r.entryTicks, i => new CheckpointEagerly());
                var min = parallelMin.Min(r => true, r => r.Second, i => new CheckpointEagerly());

                var parallelMax = input
                    .Where(r => r.entryTicks > 0).SetCheckpointPolicy(i => new CheckpointEagerly())
                    .Max(r => r.key.GetHashCode() % workerCount, r => r.entryTicks, i => new CheckpointEagerly());
                var max = parallelMax.Max(r => true, r => r.Second, i => new CheckpointEagerly());

                var consumable = min
                    .Join(max, mi => mi.First, ma => ma.First, (mi, ma) => mi.Second.PairWith(ma.Second)).SetCheckpointPolicy(i => new CheckpointEagerly());
                var window = consumable.SetCheckpointType(CheckpointType.StatelessLogAll).SetCheckpointPolicy(i => new CheckpointEagerly());

                return window;
            }

            private static Record FillFromCC(Record r, IntPair c)
            {
                r.otherKey = c.t;
                return r;
            }

            private Collection<Record, BatchIn<Epoch>> Compute(Computation computation, Collection<Record, BatchIn<Epoch>> input)
            {
                //return input;
                using (var cp = computation.WithCheckpointPolicy(v => new CheckpointAtBatch<BatchIn<Epoch>>(2)))
                {
                    // initial labels only needed for min, as the max will be improved on anyhow.
                    var nodes = input.Select(x => new IntPair(Math.Min(x.key, x.otherKey), Math.Min(x.key, x.otherKey)))
                                     .Consolidate();

                    // symmetrize the graph
                    var edges = input
                        .Select(edge => new IntPair(edge.otherKey, edge.key))
                        .Concat(input.Select(edge => new IntPair(edge.key, edge.otherKey)));

                    // prioritization introduces labels from small to large (in batches).
                    var cc = nodes
                            .Where(x => false)
                            .FixedPoint(
                                (lc, x) => x
                                    .Join(edges.EnterLoop(lc), n => n.s, e => e.s, (n, e) => new IntPair(e.t, n.t))
                                    .Concat(nodes.EnterLoop(lc))
                                    .Min(n => n.s, n => n.t),
                                n => n.s,
                                Int32.MaxValue);

                    var doneCC = input.Join(cc, r => r.key, c => c.s, (r, c) => FillFromCC(r, c));
                    var unique = doneCC.Max(r => r.key, r => r.EntryTicks).Consolidate();
                    return unique;
                }
            }

            public int reduceStage;
            public IEnumerable<int> ToMonitor
            {
                get { return new int[] { reduceStage }; }
            }

            public void Make(Computation computation, SlowPipeline slow, FastPipeline perfect)
            {
                this.source = new SubBatchDataSource<HTRecord, BatchIn<Epoch>>();

                Placement ccPlacement =
                    new Placement.ProcessRange(Enumerable.Range(this.baseProc, this.range),
                        Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));
                Placement slowPlacement =
                    new Placement.ProcessRange(Enumerable.Range(slow.baseProc, slow.range),
                        Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));

                var slowOutput = slow.Make(computation);

                Stream<Pair<long, long>, BatchIn<Epoch>> ccWindow;

                using (var p = computation.WithPlacement(ccPlacement))
                {
                    var forCC = computation.BatchedEntry<Record, Epoch>(c =>
                        {
                            Collection<Record, BatchIn<Epoch>> cc;

                            var reduced = computation
                                .BatchedEntry<Record, BatchIn<Epoch>>(ic =>
                                    {
                                        Stream<HTRecord, BatchIn<BatchIn<Epoch>>> input;
                                        // all the batches come from the slow vertices
                                        using (var inputs = computation.WithPlacement(slowPlacement))
                                        {
                                            input = computation
                                                .NewInput(this.source)
                                                .SetCheckpointType(CheckpointType.CachingInput)
                                                .SetCheckpointPolicy(v => new CheckpointEagerly());
                                        }
                                        return this.Reduce(input);
                                    }, "CCPipeLineExitInnerBatch");

                            var asCollection = reduced.Select(r =>
                                {
                                    if (r.EntryTicks < 0)
                                    {
                                        r.EntryTicks = -r.EntryTicks;
                                        return new Weighted<Record>(r, -1);
                                    }
                                    else
                                    {
                                        return new Weighted<Record>(r, 1);
                                    }
                                }).AsCollection(false);

                            cc = this.Compute(computation, asCollection);

                            ccWindow = this.TimeWindow(reduced, ccPlacement.Count);

                            Stream<SlowPipeline.Record, BatchIn<Epoch>> slowData;
                            Stream<Pair<long, long>, BatchIn<Epoch>> slowWindow;

                            using (var pp = computation.WithPlacement(slowPlacement))
                            {
                                slowData = c.EnterBatch(slowOutput.First);
                                slowWindow = c.EnterBatch(slowOutput.Second);
                            }

                            perfect.Make(computation, slowData, slowWindow, slowPlacement, cc, ccWindow, ccPlacement);

                            return cc;
                        }, "CCPipeLineExitOuterBatch");
                }
            }

            public CCPipeline(int baseProc, int range)
            {
                this.baseProc = baseProc;
                this.range = range;
            }
        }

        public class PartitionedActionVertex<R> : SinkVertex<Pair<int, R>, Epoch>
        {
            private readonly Action<R> action;

            public override void OnReceive(Message<Pair<int, R>, Epoch> message)
            {
                for (int i = 0; i < message.length; ++i)
                {
                    action(message.payload[i].Second);
                }
            }

            private PartitionedActionVertex(int index, Stage<Epoch> stage, Action<R> action)
                : base(index, stage)
            {
                this.action = action;
            }

            public static void PartitionedActionStage(Stream<Pair<int, R>, Epoch> stream, Action<R> action)
            {
                stream.NewSinkStage<Pair<int, R>, Epoch>((i, s) => new PartitionedActionVertex<R>(i, s, action), null, "PartitionedAction")
                    .SetCheckpointType(CheckpointType.None);
            }
        }

        public class PartitionedActionVertex : SinkVertex<int, Epoch>
        {
            private readonly Action action;

            public override void OnReceive(Message<int, Epoch> message)
            {
                for (int i = 0; i < message.length; ++i)
                {
                    action();
                }
            }

            private PartitionedActionVertex(int index, Stage<Epoch> stage, Action action)
                : base(index, stage)
            {
                this.action = action;
            }

            public static void PartitionedActionStage(Stream<int, Epoch> stream, Action action)
            {
                stream.NewSinkStage<int, Epoch>((i, s) => new PartitionedActionVertex(i, s, action), null, "PartitionedAction")
                    .SetCheckpointType(CheckpointType.None);
            }
        }

        public struct HTRecord : IEquatable<HTRecord>
        {
            public int key;
            public int otherKey;
            public long entryTicks;

            public bool Equals(HTRecord other)
            {
                return key == other.key && otherKey == other.otherKey && entryTicks == other.entryTicks;
            }
        }

        private Configuration config;
        private StreamWriter checkpointLog = null;
        internal StreamWriter CheckpointLog
        {
            get
            {
                if (checkpointLog == null)
                {
                    string fileName = String.Format("inputLatency.{0:D3}.log", this.config.ProcessID);
                    checkpointLog = this.config.LogStreamFactory(fileName).Log;
                }
                return checkpointLog;
            }
        }

        public void WriteLog(string entry)
        {
            var log = this.CheckpointLog;
            lock (log)
            {
                log.WriteLine(entry);
            }
        }

        public class BatchMaker
        {
            private readonly int processId;
            private readonly int processes;

            // We ensure that each key has at least one edge present that is never removed, and those are introduced
            // using this method
            private IEnumerable<HTRecord> MakeAllKeyBatch(Random random, long entryTicks)
            {
                for (int i = (this.processId % this.processes); i < Program.numberOfKeys; i += this.processes)
                {
                    yield return new HTRecord
                    {
                        key = i,
                        otherKey = random.Next(numberOfKeys),
                        entryTicks = entryTicks
                    };
                }
            }

            // keep track of the times of batches we put in, so we can remove the exact same data later
            private readonly Queue<long> batchTimes = new Queue<long>();
            private Random introduceRandom;
            private Random removeRandom;

            private IEnumerable<HTRecord> MakeBatch(Random random, int batchSize, long entryTicks)
            {
                for (int i = 0; i < batchSize; ++i)
                {
                    yield return new HTRecord
                    {
                        key = random.Next(Program.numberOfKeys),
                        otherKey = random.Next(Program.numberOfKeys),
                        entryTicks = entryTicks
                    };
                }
            }

            private int batchesReturned = 0;

            public Pair<IEnumerable<HTRecord>, IEnumerable<HTRecord>> NextBatch(long entryTicks)
            {
                IEnumerable<HTRecord> inBatch, outBatch = new HTRecord[0];

                if (batchesReturned == 0)
                {
                    Random thisProcessRandom = new Random();
                    int randomSeed = thisProcessRandom.Next();

                    // make matching random number generators for adding and removing records
                    this.introduceRandom = new Random(randomSeed);
                    this.removeRandom = new Random(randomSeed);
                }

                // the batch is being added, so save its time
                this.batchTimes.Enqueue(entryTicks);

                inBatch = this.MakeAllKeyBatch(this.introduceRandom, entryTicks).Concat(this.MakeBatch(this.introduceRandom, Program.htBatchSize, entryTicks)).ToArray();

                if (this.batchesReturned >= Program.htInitialBatches)
                {
                    // the batch is being removed, so look up the time that it was put in
                    entryTicks = -(this.batchTimes.Dequeue());
                    outBatch = this.MakeAllKeyBatch(this.removeRandom, entryTicks).Concat(this.MakeBatch(this.removeRandom, Program.htBatchSize, entryTicks)).ToArray();
                }

                ++this.batchesReturned;
                return inBatch.PairWith(outBatch);
            }

            public BatchMaker(int processes, int processId)
            {
                this.processes = processes;
                this.processId = processId;
            }
        };

        private int currentCompletedSlowEpoch = -1;

        public void AcceptSlowStableTime(Epoch slowTime)
        {

            lock (this)
            {
                if (slowTime.epoch > this.currentCompletedSlowEpoch)
                {
                    this.currentCompletedSlowEpoch = slowTime.epoch;
                    Console.WriteLine("Slow stable epoch " + this.currentCompletedSlowEpoch);
                }
            }
        }

        public void AcceptCCReduceStableTime(Pointstamp stamp)
        {
            KeyValuePair<BatchIn<BatchIn<Epoch>>, long>[] earlier;
            lock (this.ccBatchEntryTime)
            {
                earlier = this.ccBatchEntryTime
                    .Where(b => FTFrontier.IsLessThanOrEqualTo(b.Key.ToPointstamp(stamp.Location), stamp))
                    .ToArray();
                foreach (var batch in earlier.Select(b => b.Key))
                {
                    this.ccBatchEntryTime.Remove(batch);
                }
            }

            long now = DateTime.Now.Ticks;
            foreach (var ccBatch in earlier)
            {
                double latency = (double)(now - ccBatch.Value) / (double)TimeSpan.TicksPerMillisecond;
                this.WriteLog("C" + ccBatch.Key + " " + latency);
                Console.WriteLine("CC reduce " + ccBatch.Key + " " + ccBatch.Value + "->" + now + ": " + latency);
            }
        }

        public void AcceptSlowReduceStableTime(Pointstamp stamp)
        {
            KeyValuePair<BatchIn<Epoch>, long>[] earlier;
            lock (this.slowBatchEntryTime)
            {
                earlier = this.slowBatchEntryTime
                    .Where(b => FTFrontier.IsLessThanOrEqualTo(b.Key.ToPointstamp(stamp.Location), stamp))
                    .ToArray();
                foreach (var batch in earlier.Select(b => b.Key))
                {
                    this.slowBatchEntryTime.Remove(batch);
                }
            }

            long now = DateTime.Now.Ticks;
            foreach (var slowBatch in earlier)
            {
                double latency = (double)(now - slowBatch.Value) / (double)TimeSpan.TicksPerMillisecond;
                this.WriteLog("S" + slowBatch.Key + " " + latency);
                Console.WriteLine("Slow reduce " + slowBatch.Key + " " + slowBatch.Value + "->" + now + ": " + latency);
            }
        }

        void HighThroughputBatchInitiator()
        {
            long nowMs = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            long nextSlowBatch = nowMs + Program.slowBatchTime;
            long nextCCBatch = nowMs + Program.ccBatchTime;

            Epoch sendingSlowBatch = new Epoch(0);
            int slowSubBatch = 0;
            BatchIn<Epoch> sendingCCBatch = new BatchIn<Epoch>(new Epoch(0), 0);
            int CCSubBatch = 0;

            while (true)
            {
                long now = DateTime.Now.Ticks;
                nowMs = now / TimeSpan.TicksPerMillisecond;

                if (nowMs > nextSlowBatch)
                {
                    sendingSlowBatch = new Epoch(sendingSlowBatch.epoch + 1);
                    slowSubBatch = 0;
                    nextSlowBatch += Program.slowBatchTime;
                }

                if (nowMs > nextCCBatch)
                {
                    sendingCCBatch = new BatchIn<Epoch>(sendingCCBatch.outerTime, sendingCCBatch.batch + 1);
                    CCSubBatch = 0;
                    nextCCBatch += Program.ccBatchTime;
                }

                lock (this)
                {
                    if (this.currentCompletedSlowEpoch > sendingCCBatch.outerTime.epoch)
                    {
                        sendingCCBatch = new BatchIn<Epoch>(new Epoch(this.currentCompletedSlowEpoch), 0);
                    }
                }

                lock (this.slowBatchEntryTime)
                {
                    BatchIn<Epoch> slowBatch;
                    slowBatch.outerTime = sendingSlowBatch;
                    slowBatch.batch = slowSubBatch;
                    this.slowBatchEntryTime.Add(slowBatch, now);
                }

                lock (this.ccBatchEntryTime)
                {
                    BatchIn<BatchIn<Epoch>> ccBatch;
                    ccBatch.outerTime = sendingCCBatch;
                    ccBatch.batch = CCSubBatch;
                    this.ccBatchEntryTime.Add(ccBatch, now);
                }

                Console.WriteLine("Sending slow " + sendingSlowBatch + " cc " + sendingCCBatch);

                // tell each input worker to start the next batch
                this.batchCoordinator.OnNext(Enumerable
                    .Range(0, Program.slowRange)
                    .Select(i => i.PairWith(now.PairWith(sendingSlowBatch.PairWith(sendingCCBatch)))));

                ++slowSubBatch;
                ++CCSubBatch;

                Thread.Sleep(Program.htSleepTime);
            }
        }

        private BatchMaker batchMaker;
        private Epoch currentSlowBatch = new Epoch(0);
        private BatchIn<Epoch> currentCCBatch = new BatchIn<Epoch>(new Epoch(0), 0);

        private readonly Dictionary<BatchIn<Epoch>, long> slowBatchEntryTime = new Dictionary<BatchIn<Epoch>, long>();
        private readonly Dictionary<BatchIn<BatchIn<Epoch>>, long> ccBatchEntryTime = new Dictionary<BatchIn<BatchIn<Epoch>>, long>();

        void SendBatch(long entryTicks, Epoch slowBatch, BatchIn<Epoch> ccBatch)
        {
            var batch = this.batchMaker.NextBatch(entryTicks);

            // tell each slow worker to start the next batch
            if (!this.currentSlowBatch.Equals(slowBatch))
            {
                this.slow.source.CompleteOuterBatch(new Epoch(slowBatch.epoch - 1));
                this.currentSlowBatch = slowBatch;
            }

            this.slow.source.OnNext(batch.First);
            
            // tell each CC worker to start the next batch
            if (!this.currentCCBatch.Equals(ccBatch))
            {
                if (ccBatch.batch == 0)
                {
                    this.cc.source.CompleteOuterBatch(new BatchIn<Epoch>(new Epoch(ccBatch.outerTime.epoch - 1), int.MaxValue));
                }
                else
                {
                    this.cc.source.CompleteOuterBatch(new BatchIn<Epoch>(ccBatch.outerTime, ccBatch.batch - 1));
                }
                this.currentCCBatch = ccBatch;
            }

            this.cc.source.OnNext(batch.First.Concat(batch.Second));
        }

        private void StartBatches()
        {
            var thread = new System.Threading.Thread(new System.Threading.ThreadStart(this.HighThroughputBatchInitiator));
            thread.Start();
        }

        private void ReactToStable(object o, StageStableEventArgs args)
        {
            Pointstamp stamp = args.frontier[0];
            if (args.stageId == this.perfect.slowStage)
            {
                Epoch slowTime = new Epoch(stamp.Timestamp.a);
                if (stamp.Timestamp.b >= Int32.MaxValue - 1 && stamp.Timestamp.c >= Int32.MaxValue - 1)
                {
                    this.AcceptSlowStableTime(slowTime);
                    this.perfect.AcceptSlowDataStable(slowTime);
                }
                else if (slowTime.epoch > 0)
                {
                    slowTime = new Epoch(slowTime.epoch - 1);
                    this.AcceptSlowStableTime(slowTime);
                    this.perfect.AcceptSlowDataStable(slowTime);
                }
            }
            else if (args.stageId == this.perfect.ccStage)
            {
                BatchIn<Epoch> ccTime = new BatchIn<Epoch>(new Epoch(stamp.Timestamp.a), stamp.Timestamp.b);
                if (stamp.Timestamp.c >= Int32.MaxValue - 1)
                {
                    this.perfect.AcceptCCDataStable(ccTime);
                }
                else if (ccTime.batch > 0)
                {
                    ccTime = new BatchIn<Epoch>(new Epoch(stamp.Timestamp.a), stamp.Timestamp.b - 1);
                    this.perfect.AcceptCCDataStable(ccTime);
                }
                else if (ccTime.outerTime.epoch > 0)
                {
                    ccTime = new BatchIn<Epoch>(new Epoch(stamp.Timestamp.a-1), Int32.MaxValue - 1);
                    this.perfect.AcceptCCDataStable(ccTime);
                }
            }
            else if (args.stageId == this.perfect.resultStage)
            {
                this.perfect.ReleaseOutputs(stamp);
            }
            else if (args.stageId == this.cc.reduceStage)
            {
                this.AcceptCCReduceStableTime(stamp);
            }
            else if (args.stageId == this.slow.reduceStage)
            {
                this.AcceptSlowReduceStableTime(stamp);
            }
        }

#if false
        static private int slowBase = 1;
        static private int slowRange = 10;
        static private int ccBase = 11;
        static private int ccRange = 20;
        static private int fbBase = 31;
        static private int fbRange = 5;
        static private int fpBase = 36;
        static private int fpRange = 5;
        static private int numberOfKeys = 10000;
        static private int fastBatchSize = 1;
        static private int fastSleepTime = 100;
        static private int ccBatchTime = 1000;
        static private int slowBatchTime = 60000;
        static private int htBatchSize = 100;
        static private int htInitialBatches = 100;
        static private int htSleepTime = 1000;
#else
#if true
        static private int slowBase = 0;
        static private int slowRange = 2;
        static private int ccBase = 2;
        static private int ccRange = 1;
        //static private int fbBase = 1;
        //static private int fbRange = 1;
        static private int fpBase = 3;
        static private int fpRange = 1;
#if false
        static private int numberOfKeys = 10000;
        static private int fastBatchSize = 10;
        static private int fastSleepTime = 1000;
        static private int ccBatchTime = 5000;
        static private int slowBatchTime = 60000;
        static private int htBatchSize = 10;
        static private int htInitialBatches = 100;
        static private int htSleepTime = 1000;
#else
        static private int numberOfKeys = 100;
        static private int fastBatchSize = 1;
        static private int fastSleepTime = 1000;
        static private int ccBatchTime = 5000;
        static private int slowBatchTime = 20000;
        static private int htBatchSize = 10;
        static private int htSleepTime = 1000;
        static private int htInitialBatches = 10;
#endif
#else
        static private int slowBase = 0;
        static private int slowRange = 1;
        static private int ccBase = 0;
        static private int ccRange = 1;
        static private int fbBase = 0;
        static private int fbRange = 1;
        static private int fpBase = 0;
        static private int fpRange = 1;
        static private int numberOfKeys = 10;
        static private int fastBatchSize = 1;
        static private int fastSleepTime = 1000;
        static private int ccBatchTime = 2000;
        static private int slowBatchTime = 4000;
        static private int htBatchSize = 100;
        static private int htSleepTime = 1000;
        static private int htInitialBatches = 10;
#endif
#endif

        static private Program program;
        private Computation computation;
        private SlowPipeline slow;
        private CCPipeline cc;
        private FastPipeline perfect;
        private BatchedDataSource<Pair<int, Pair<long, Pair<Epoch, BatchIn<Epoch>>>>> batchCoordinator;

        private class FileLogStream : LogStream
        {
            private StreamWriter log;
            private FileStream logFile;
            public StreamWriter Log
            {
                get { return log; }
            }

            public void Flush()
            {
                lock (log)
                {
                    log.Flush();
                    logFile.Flush(true);
                }
            }

            private void FlushFileThread()
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    this.Flush();
                }
            }

            public FileLogStream(string prefix, string fileName)
            {
                this.logFile = new FileStream(Path.Combine(prefix, fileName), FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                this.log = new StreamWriter(this.logFile);
                var flush = new System.Threading.Thread(
                    new System.Threading.ThreadStart(() => this.FlushFileThread()));
                flush.Start();
            }
        }

        public void Execute(string[] args)
        {
            this.config = Configuration.FromArgs(ref args);
            this.config.MaxLatticeInternStaleTimes = 10;

            bool useAzure = false;
            string logPrefix = "";
            int i = 0;
            while (i < args.Length)
            {
                switch (args[i].ToLower())
                {
                    case "-azure":
                        useAzure = true;
                        ++i;
                        break;

                    case "-sizes":
                        slowBase = 1;
                        slowRange = Int32.Parse(args[i + 1]);
                        ccBase = slowBase + slowRange;
                        ccRange = Int32.Parse(args[i + 2]);
                        fpBase = ccBase + ccRange;
                        fpRange = Int32.Parse(args[i + 3]);
                        i += 4;
                        break;

                    case "-log":
                        logPrefix = args[i + 1];
                        i += 2;
                        break;
                }
            }

            System.IO.Directory.CreateDirectory(logPrefix);
            this.config.LogStreamFactory = (s => new FileLogStream(logPrefix, s));

            FTManager manager = new FTManager(this.config.LogStreamFactory);

            if (useAzure)
            {
                if (accountName == null)
                {
                    var defaultAccount = Microsoft.Research.Naiad.Frameworks.Azure.Helpers.DefaultAccount(this.config);
                    accountName = defaultAccount.Credentials.AccountName;
                    accountKey = defaultAccount.Credentials.ExportBase64EncodedKey();
                }
                this.config.CheckpointingFactory = s => new AzureStreamSequence(accountName, accountKey, containerName, s);
            }
            else
            {
                System.IO.Directory.CreateDirectory("checkpoint");
                this.config.CheckpointingFactory = s => new FileStreamSequence("checkpoint", s);
            }

            this.config.DefaultCheckpointInterval = 1000;

            using (var computation = NewComputation.FromConfig(this.config))
            {
                Placement inputPlacement = new Placement.ProcessRange(Enumerable.Range(slowBase, slowRange), Enumerable.Range(0, 1));
                Placement batchTriggerPlacement = new Placement.ProcessRange(Enumerable.Range(fpBase, 1), Enumerable.Range(0, 1));

                if (inputPlacement.Select(x => x.ProcessId).Contains(this.config.ProcessID))
                {
                    this.batchMaker = new BatchMaker(slowRange, this.config.ProcessID - slowBase);
                }
                else
                {
                    this.batchMaker = null;
                }

                this.computation = computation;
                this.slow = new SlowPipeline(slowBase, slowRange);
                this.cc = new CCPipeline(ccBase, ccRange);
                //this.buggy = new FastPipeline(slowBase, fbBase, fbRange);
                this.perfect = new FastPipeline(this.config, fpBase, fpBase, fpRange);

                this.batchCoordinator = new BatchedDataSource<Pair<int, Pair<long, Pair<Epoch, BatchIn<Epoch>>>>>();
                using (var bTrigger = computation.WithPlacement(batchTriggerPlacement))
                {
                    var batchTrigger = computation.NewInput(this.batchCoordinator).SetCheckpointType(CheckpointType.None);
                    using (var bSend = computation.WithPlacement(inputPlacement))
                    {
                        batchTrigger
                            .PartitionBy(x => x.First).SetCheckpointType(CheckpointType.None)
                            .PartitionedActionStage(x => this.SendBatch(x.First, x.Second.First, x.Second.Second));
                    }
                }
                this.cc.Make(computation, this.slow, this.perfect);

                if (this.config.ProcessID == 0)
                {
                    manager.Initialize(computation, this.slow.ToMonitor.Concat(this.cc.ToMonitor.Concat(this.perfect.ToMonitor)).Distinct());
                }

                //computation.OnStageStable += (x, y) => { Console.WriteLine(y.stageId + " " + y.frontier[0]); };

                computation.Activate();

                if (this.config.ProcessID == fpBase)
                {
                    var stopwatch = computation.Controller.Stopwatch;
                    HashSet<Pointstamp> previousFrontier = new HashSet<Pointstamp>();
                    computation.OnFrontierChange += (x, y) =>
                    {
                        long ticks = stopwatch.ElapsedTicks;
                        long microSeconds = (ticks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                        HashSet<Pointstamp> newSet = new HashSet<Pointstamp>();
                        foreach (var f in y.NewFrontier) { newSet.Add(f); }
                        var added = newSet.Where(f => !previousFrontier.Contains(f));
                        var removed = previousFrontier.Where(f => !newSet.Contains(f));
                        Console.WriteLine(String.Format("{0:D11}\t", microSeconds) + " +" + string.Join(", ", added) + " -" + string.Join(", ", removed));
                        Console.Out.Flush();
                        previousFrontier = newSet;
                    };
                    computation.OnStageStable += this.ReactToStable;
                    this.StartBatches();
                }
                else
                {
                    this.batchCoordinator.OnCompleted();
                }

                if (this.config.ProcessID == 0)
                {
                    IEnumerable<int> failSlow = Enumerable.Range(slowBase, slowRange);
                    IEnumerable<int> failMedium =
                        //Enumerable.Range(ccBase, ccRange).Concat(Enumerable.Range(fbBase, fbRange)).Distinct()
                        Enumerable.Range(ccBase, ccRange)
                        .Except(failSlow);
                    IEnumerable<int> failFast = Enumerable.Range(fpBase, fpRange)
                        .Except(failSlow.Concat(failMedium));

                    while (true)
                    {
                        System.Threading.Thread.Sleep(Timeout.Infinite);
                        System.Threading.Thread.Sleep(15000);
                        if (this.config.Processes > 2)
                        {
                            manager.FailProcess(1);
                        }

                        manager.PerformRollback(failSlow, failMedium, failFast);
                    }
                }

                Thread.Sleep(Timeout.Infinite);

                computation.Join();
            }
        }

        static void Main(string[] args)
        {
            program = new Program();
            program.Execute(args);
        }
    }
}
