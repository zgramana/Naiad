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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
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

        public static Pair<Stream<Program.FastPipeline.Record, TInner>, HashSet<Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>>>
            StaggeredJoin<TInput1, TInput2, TKey, TInner, TOuter>(
            this Stream<TInput1, TInner> stream1,
            Stream<TInput2, TInner> stream2,
            Stream<Pair<long, long>, TInner> stream2Window,
            Func<TInput1, TKey> keySelector1, Func<TInput2, TKey> keySelector2,
            Func<TInput1, TInput2, Pair<long, long>, Program.FastPipeline.Record> resultSelector,
            Func<TInner, TOuter> timeSelector, Action<TOuter> filledAction,
            string name)
            where TInput2 : Program.IRecord
            where TInner : Time<TInner>
            where TOuter : Time<TOuter>
        {
            HashSet<Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>> vertices =
                new HashSet<Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>>();

            return stream1.NewTernaryStage(stream2.Prepend(), stream2Window, (i, s) =>
                {
                    var vertex = new Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>(
                        i, s, keySelector1, keySelector2, resultSelector, timeSelector, filledAction);
                    vertices.Add(vertex);
                    return vertex;
                },
                    x => keySelector1(x).GetHashCode(), x => keySelector2(x).GetHashCode(), x => 0, null, name)
                    .SetCheckpointType(CheckpointType.StatelessLogEphemeral).SetCheckpointPolicy(s => new CheckpointWithoutPersistence())
                    .PairWith(vertices);
        }

        public static Stream<R, T> Prepend<R, T>(this Stream<R, T> stream)
            where R : Program.IRecord
            where T : Time<T>
        {
            return stream.NewUnaryStage((i, s) =>
                new Program.FastPipeline.PrependVertex<R, T>(i, s), null, null, "Prepend")
                    .SetCheckpointType(CheckpointType.StatelessLogEphemeral).SetCheckpointPolicy(s => new CheckpointWithoutPersistence());
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
        private string accountName = "";
        private string accountKey = "";
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
                    return key + " " + count + " " + entryTicks;
                }
            }

            public Stream<Record, IterationIn<Epoch>> Reduce(Stream<HTRecord, IterationIn<Epoch>> input)
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

            public Stream<Record, Epoch> Compute(Stream<Record, Epoch> input)
            {
                return input;
            }

            public int reduceStage;
            public IEnumerable<int> ToMonitor
            {
                get { return new int[] { reduceStage }; }
            }

            public Pair<Collection<Record, Epoch>, Stream<Pair<long, long>, Epoch>> Make(Computation computation)
            {
                this.source = new SubBatchDataSource<HTRecord, Epoch>();

                Placement placement =
                    new Placement.ProcessRange(Enumerable.Range(this.baseProc, this.range),
                        Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));

                Collection<Record, Epoch> weighted;
                Stream<Pair<long, long>, Epoch> window;

                using (var p = computation.WithPlacement(placement))
                {
                    var reduced = computation.BatchedEntry<Record, Epoch>(c =>
                            {
                                var input = computation.NewInput(this.source)
                                    .SetCheckpointType(CheckpointType.CachingInput)
                                    .SetCheckpointPolicy(s => new CheckpointEagerly());
                                return this.Reduce(input);
                            });

                    var computed = this.Compute(reduced);

                    weighted = computed
                        .Select(r =>
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

                    window = this.TimeWindow(reduced, placement.Count);
                }

                return weighted.PairWith(window);
            }

            public SlowPipeline(int baseProc, int range)
            {
                this.baseProc = baseProc;
                this.range = range;
            }
        }

        public class FastPipeline
        {
            private FileStream checkpointLogFile = null;
            private StreamWriter checkpointLog = null;
            internal StreamWriter CheckpointLog
            {
                get
                {
                    if (checkpointLog == null)
                    {
                        string fileName = String.Format("fastPipe.{0:D3}.log", processId);
                        this.checkpointLogFile = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                        this.checkpointLog = new StreamWriter(this.checkpointLogFile);
                        var flush = new System.Threading.Thread(new System.Threading.ThreadStart(() => FlushThread()));
                        flush.Start();
                    }
                    return checkpointLog;
                }
            }

            private void FlushThread()
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    lock (this)
                    {
                        if (this.checkpointLog != null)
                        {
                            this.checkpointLog.Flush();
                            this.checkpointLogFile.Flush(true);
                        }
                    }
                }
            }

            public void WriteLog(string entry, params object[] args)
            {
                lock (this)
                {
                    this.CheckpointLog.WriteLine(entry, args);
                }
            }

            public int processId;
            public int baseProc;
            public int range;

            public class PrependVertex<R, T> : UnaryVertex<R, R, T> where T : Time<T> where R : IRecord
            {
                private HashSet<T> seenAny = new HashSet<T>();

                public override void OnReceive(Message<R, T> message)
                {
                    var output = this.Output.GetBufferForTime(message.time);
                    if (!seenAny.Contains(message.time))
                    {
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

            public class StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter> : TernaryVertex<TInput1, TInput2, Pair<long, long>, Record, TInner>
                where TInput2 : IRecord
                where TInner : Time<TInner>
                where TOuter : Time<TOuter>
            {
                private readonly Dictionary<TOuter, Dictionary<TKey, List<TInput2>>> values = new Dictionary<TOuter,Dictionary<TKey,List<TInput2>>>();
                private readonly Dictionary<TOuter, Pair<long, long>> windows = new Dictionary<TOuter, Pair<long, long>>();
                private readonly HashSet<TOuter> gotValues = new HashSet<TOuter>();

                private readonly Func<TInput1, TKey> keySelector1;
                private readonly Func<TInput2, TKey> keySelector2;
                private readonly Func<TInput1, TInput2, Pair<long, long>, Record> resultSelector;
                private readonly Func<TInner, TOuter> timeSelector;
                private readonly Action<TOuter> filledAction;

                protected override bool CanRollBackPreservingState(Pointstamp[] frontier)
                {
                    return true;
                }

                public override void RollBackPreservingState(Pointstamp[] frontier, ICheckpoint<TInner> lastFullCheckpoint, ICheckpoint<TInner> lastIncrementalCheckpoint)
                {
                    base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);
                }

                public override void OnReceive1(Message<TInput1, TInner> message)
                {
                    Console.WriteLine(this.Stage.Name + " Receive1 " + message.time);
                    TOuter outer = timeSelector(message.time);

                    Dictionary<TKey, List<TInput2>> currentValues;
                    Pair<long, long> window;

                    lock (this)
                    {
                        currentValues = this.values[outer];
                        window = this.windows[outer];
                    }

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
                    }
                }

                public override void OnReceive2(Message<TInput2, TInner> message)
                {
                    Console.WriteLine(this.Stage.Name + " Receive2 " + message.time);
                    TOuter outerTime = timeSelector(message.time);

                    int baseRecord = 0;

                    Dictionary<TKey, List<TInput2>> currentValues;

                    lock (this)
                    {
                        if (message.payload[0].EntryTicks == -1)
                        {
                            ++baseRecord;
                            if (this.values.ContainsKey(outerTime))
                            {
                                throw new ApplicationException("Duplicate times");
                            }
                            else
                            {
                                this.values.Add(outerTime, new Dictionary<TKey, List<TInput2>>());
                            }
                        }

                        currentValues = this.values[outerTime];
                    }

                    for (int i = baseRecord; i < message.length; i++)
                    {
                        if (message.payload[i].EntryTicks == -2)
                        {
                            if (this.gotValues.Contains(outerTime))
                            {
                                this.gotValues.Remove(outerTime);
                                this.filledAction(outerTime);
                            }
                            else
                            {
                                this.gotValues.Add(outerTime);
                            }

                            Console.WriteLine(this.Stage.Name + " R2 " + outerTime + ": " + currentValues.Select(k => k.Value.Count).Sum());
                            //foreach (var k in currentValues.Keys)
                            //{
                                //Console.WriteLine(this.Stage.Name + " R2 " + k + ": " + currentValues[k].Count);
                                //foreach (var v in currentValues[k])
                                //{
                                //    Console.WriteLine("  " + this.Stage.Name + " R2 " + v + " ");
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

                public override void OnReceive3(Message<Pair<long, long>, TInner> message)
                {
                    Console.WriteLine(this.Stage.Name + " receive window " + message.time + message.payload[0]);
                    TOuter outerTime = timeSelector(message.time);

                    lock (this)
                    {
                        this.windows[outerTime] = message.payload[0];
                    }

                    if (this.gotValues.Contains(outerTime))
                    {
                        this.gotValues.Remove(outerTime);
                        this.filledAction(outerTime);
                    }
                    else
                    {
                        this.gotValues.Add(outerTime);
                    }
                }

                public void RemoveTimesBelow(TOuter outerTime)
                {
                    Pointstamp outerStamp = outerTime.ToPointstamp(0);
                    lock (this)
                    {
                        foreach (var time in this.values.Keys.ToArray())
                        {
                            Pointstamp stamp = time.ToPointstamp(0);

                            if (!stamp.Equals(outerStamp) && FTFrontier.IsLessThanOrEqualTo(stamp, outerStamp))
                            {
                                Console.WriteLine("Removing " + time);
                                this.values.Remove(time);
                            }
                        }
                        foreach (var time in this.windows.Keys.ToArray())
                        {
                            Pointstamp stamp = time.ToPointstamp(0);

                            if (!stamp.Equals(outerStamp) && FTFrontier.IsLessThanOrEqualTo(stamp, outerStamp))
                            {
                                Console.WriteLine("Removing window " + time);
                                this.windows.Remove(time);
                            }
                        }
                    }
                }

                public StaggeredJoinVertex(int index, Stage<TInner> stage,
                    Func<TInput1, TKey> key1, Func<TInput2, TKey> key2,
                    Func<TInput1, TInput2, Pair<long, long>, Record> result,
                    Func<TInner, TOuter> timeSelector, Action<TOuter> filledAction)
                    : base(index, stage)
                {
                    this.values = new Dictionary<TOuter, Dictionary<TKey, List<TInput2>>>();
                    this.keySelector1 = key1;
                    this.keySelector2 = key2;
                    this.resultSelector = result;
                    this.timeSelector = timeSelector;
                    this.filledAction = filledAction;
                }
            }

            public class ExitVertex : UnaryVertex<Record, Record, IterationIn<IterationIn<Epoch>>>
            {
                private readonly FastPipeline parent;

                public override void OnReceive(Message<Record, IterationIn<IterationIn<Epoch>>> message)
                {
                    parent.HoldOutputs(message);
                    this.NotifyAt(message.time);
                }

                public override void OnNotify(IterationIn<IterationIn<Epoch>> time)
                {
                    foreach (var vertex in this.parent.slowVertices)
                    {
                        vertex.RemoveTimesBelow(time.outerTime.outerTime);
                    }
                    foreach (var vertex in this.parent.ccVertices)
                    {
                        vertex.RemoveTimesBelow(time.outerTime);
                    }
                }

                private ExitVertex(int index, Stage<IterationIn<IterationIn<Epoch>>> stage, FastPipeline parent)
                    : base(index, stage)
                {
                    this.parent = parent;
                }

                public static Stream<Record, IterationIn<IterationIn<Epoch>>> ExitStage(
                    Stream<Record, IterationIn<IterationIn<Epoch>>> stream,
                    FastPipeline parent)
                {
                    return stream.NewUnaryStage<Record, Record, IterationIn<IterationIn<Epoch>>>(
                        (i,s) => new ExitVertex(i, s, parent), null, null, "ExitFastPipeline")
                        .SetCheckpointType(CheckpointType.Stateless);
                }
            }

            public struct Record : IEquatable<Record>
            {
                public int homeProcess;
                public long startMs;
                public Pair<long, long> slowWindow;
                public Pair<long, long> ccWindow;
                public int slowJoinKey;
                public int ccJoinKey;

                public bool Equals(Record other)
                {
                    return homeProcess == other.homeProcess &&
                        startMs == other.startMs &&
                        slowWindow.Equals(other.slowWindow) &&
                        ccWindow.Equals(other.ccWindow) &&
                        slowJoinKey == other.slowJoinKey &&
                        ccJoinKey == other.ccJoinKey;
                }
            }

            private Epoch slowTime;
            private bool gotSlowTime = false;
            private bool gotCCTime = false;

            public void AcceptSlowTime(Epoch slowTime)
            {
                lock (this)
                {
                    Console.WriteLine("Fast got slow time " + slowTime);
                    this.slowTime = slowTime;
                    this.gotSlowTime = true;
                }
            }

            public void AcceptCCTime(IterationIn<Epoch> ccTime)
            {
                bool start = false;

                Console.WriteLine("Fast got CC time " + ccTime);

                if (ccTime.iteration == int.MaxValue)
                {
                    return;
                }

                lock (this)
                {
                    if (this.gotSlowTime && ccTime.outerTime.epoch >= this.slowTime.epoch)
                    {
                        if (ccTime.outerTime.epoch > this.slowTime.epoch)
                        {
                            ccTime = new IterationIn<Epoch>(this.slowTime, int.MaxValue);
                        }

                        Console.WriteLine("Setting new cc time " + ccTime);
                        this.dataSource.StartOuterBatch(ccTime);

                        if (!this.gotCCTime)
                        {
                            start = true;
                            this.gotCCTime = true;
                        }
                    }
                }

                if (start)
                {
                    //var thread = new System.Threading.Thread(new System.Threading.ThreadStart(() => FeedThread()));
                    //thread.Start();
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
                        homeProcess = this.processId,
                        startMs = ms,
                        slowWindow = (-1L).PairWith(-1L),
                        ccWindow = (-1L).PairWith(-1L),
                        slowJoinKey = random.Next(Program.numberOfKeys),
                        ccJoinKey = random.Next(Program.numberOfKeys),
                    };
                }

                for (int i=0; i<this.range; ++i)
                {
                    yield return new Record
                    {
                        homeProcess = this.processId,
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
                    Console.WriteLine("Sending fast batch");

                    this.dataSource.OnNext(this.MakeBatch(Program.fastBatchSize));
                    this.dataSource.CompleteInnerBatch();

                    Thread.Sleep(Program.fastSleepTime);
                }
            }

            private Dictionary<IterationIn<IterationIn<Epoch>>, List<Record>>
                bufferedOutputs = new Dictionary<IterationIn<IterationIn<Epoch>>, List<Record>>();

            private Pointstamp holdTime = new Pointstamp();

            private Pointstamp ToPointstamp(IterationIn<IterationIn<Epoch>> time)
            {
                Pointstamp stamp = new Pointstamp();
                stamp.Location = this.resultStage;
                stamp.Timestamp.Length = 3;
                stamp.Timestamp.a = time.outerTime.outerTime.epoch;
                stamp.Timestamp.b = time.outerTime.iteration;
                stamp.Timestamp.c = time.iteration;
                return stamp;
            }

            private void HoldOutputs(Message<Record, IterationIn<IterationIn<Epoch>>> message)
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
                    }
                    for (int i = 0; i < message.length; ++i)
                    {
                        buffer.Add(message.payload[i]);
                    }
                }
            }

            private void ReleaseOutputs(Pointstamp time)
            {
                long doneMs = program.computation.TicksSinceStartup / TimeSpan.TicksPerMillisecond;

                Console.WriteLine("Releasing records up to " + time);

                List<Pair<IterationIn<IterationIn<Epoch>>, Record>> released = new List<Pair<IterationIn<IterationIn<Epoch>>, Record>>();

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

                foreach (var record in released.Take(1))
                {
                    if (record.Second.startMs == -1)
                    {
                        this.WriteLog("-1 -1 -1");
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
                            ccBatchMs = record.Second.slowWindow.Second / TimeSpan.TicksPerMillisecond;
                        }

                        long latency = doneMs - record.Second.startMs;
                        long slowStaleness = (slowBatchMs < 0) ? -2 : doneMs - slowBatchMs;
                        long ccStaleness = (ccBatchMs < 0) ? -2 : doneMs - ccBatchMs;
                        this.WriteLog("{0:D11} {1:D11} {2:D11}", latency, slowStaleness, ccStaleness);
                        Console.WriteLine("{0:D11} {1:D11} {2:D11}", latency, slowStaleness, ccStaleness);
                    }
                }
            }

            private void ReactToStable(object o, StageStableEventArgs args)
            {
                if (args.stageId == resultStage)
                {
                    this.ReleaseOutputs(args.frontier[0]);
                }
            }

            private SubBatchDataSource<Record, IterationIn<Epoch>> dataSource;
            public int slowStage;
            public int slowWindowStage;
            private int ccStage;
            private int ccWindowStage;
            private int resultStage;

            public IEnumerable<int> ToMonitor
            {
                get { return new int[] { this.slowStage, this.ccStage, this.resultStage }; }
            }

            private Stream<R, IterationIn<Epoch>> PrepareForFast<R>(Collection<R, IterationIn<Epoch>> input, Func<R, int> partitioning)
                where R : IEquatable<R>
            {
                return input
                    .ForcePartitionBy(r => partitioning(r))
                    .ToStateless().SetCheckpointType(CheckpointType.StatefulLogEphemeral).SetCheckpointPolicy(i => new CheckpointEagerly()).Output
                    .SelectMany(r => Enumerable.Repeat(r.record, (int)Math.Max(0, r.weight))).SetCheckpointType(CheckpointType.StatelessLogEphemeral).SetCheckpointPolicy(i => new CheckpointWithoutPersistence());
            }

            private Stream<R, IterationIn<Epoch>> PrepareForFast<R>(Stream<R, IterationIn<Epoch>> input, Func<R, int> partitioning)
                where R : IEquatable<R>
            {
                return input
                    .ForcePartitionBy(r => partitioning(r))
                    .Select(r => r).SetCheckpointType(CheckpointType.StatelessLogEphemeral).SetCheckpointPolicy(i => new CheckpointWithoutPersistence());
            }

            private Stream<Record, IterationIn<IterationIn<Epoch>>> Exit(Stream<Record, IterationIn<IterationIn<Epoch>>> results,
                int placementCount)
            {
                var output = results.PartitionBy(r => r.homeProcess).SetCheckpointPolicy(i => new CheckpointWithoutPersistence());
                var exit = ExitVertex.ExitStage(output, this).SetCheckpointPolicy(i => new CheckpointWithoutPersistence());

                this.resultStage = exit.ForStage.StageId;

                return output.SelectMany(r => Enumerable.Range(0, placementCount).Select(i =>
                    {
                        r.homeProcess = i;
                        return r;
                    })).SetCheckpointPolicy(i => new CheckpointWithoutPersistence())
                    .PartitionBy(r => r.homeProcess).SetCheckpointPolicy(i => new CheckpointWithoutPersistence());
            }

            HashSet<Program.FastPipeline.StaggeredJoinVertex<Record, SlowPipeline.Record, int, IterationIn<IterationIn<Epoch>>, Epoch>> slowVertices;
            HashSet<Program.FastPipeline.StaggeredJoinVertex<Record, CCPipeline.Record, int, IterationIn<IterationIn<Epoch>>, IterationIn<Epoch>>> ccVertices;

            public void Make(Computation computation,
                Collection<SlowPipeline.Record, IterationIn<Epoch>> slowOutput,
                Stream<Pair<long, long>, IterationIn<Epoch>> slowTimeWindow,
                Collection<CCPipeline.Record, IterationIn<Epoch>> ccOutput,
                Stream<Pair<long, long>, IterationIn<Epoch>> ccTimeWindow)
            {
                this.dataSource = new SubBatchDataSource<Record, IterationIn<Epoch>>();

                Placement placement = new Placement.ProcessRange(Enumerable.Range(this.baseProc, this.range), Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));
                Placement senderPlacement = new Placement.ProcessRange(Enumerable.Range(this.baseProc, 1), Enumerable.Range(0, 1));

                using (var procs = computation.WithPlacement(placement))
                {
                    Stream<Record, IterationIn<IterationIn<Epoch>>> input;
                    using (var sender = computation.WithPlacement(senderPlacement))
                    {
                        input = computation.NewInput(dataSource).SetCheckpointType(CheckpointType.CachingInput);
                    }

                    var slow = this.PrepareForFast(slowOutput, r => r.key);
                    this.slowStage = slow.ForStage.StageId;
                    var slowWindow = this.PrepareForFast(slowTimeWindow, r => 0);
                    this.slowWindowStage = slowWindow.ForStage.StageId;
                    var cc = this.PrepareForFast(ccOutput, r => r.key);
                    this.ccStage = cc.ForStage.StageId;
                    var ccWindow = this.PrepareForFast(ccTimeWindow, r => 0);
                    this.ccWindowStage = ccWindow.ForStage.StageId;

                    var output = computation.BatchedEntry<Record, IterationIn<Epoch>>(ic =>
                        {
                            var firstJoin = input.SetCheckpointPolicy(i => new CheckpointWithoutPersistence())
                                .StaggeredJoin(
                                    ic.EnterLoop(slow).SetCheckpointPolicy(i => new CheckpointWithoutPersistence()),
                                    ic.EnterLoop(slowWindow).SetCheckpointPolicy(i => new CheckpointWithoutPersistence()),
                                    i => i.slowJoinKey, s => s.key, (i, s, w) => { i.slowWindow = w; return i; },
                                    t => t.outerTime.outerTime, this.AcceptSlowTime, "SlowJoin");

                            this.slowVertices = firstJoin.Second;

                            var secondJoin = firstJoin.First
                                .StaggeredJoin(
                                    ic.EnterLoop(cc).SetCheckpointPolicy(s => new CheckpointWithoutPersistence()),
                                    ic.EnterLoop(ccWindow).SetCheckpointPolicy(s => new CheckpointWithoutPersistence()),
                                    i => i.ccJoinKey, c => c.key, (i, c, w) => { i.ccWindow = w; return i; },
                                    t => t.outerTime, this.AcceptCCTime, "CCJoin");

                            this.ccVertices = secondJoin.Second;

                            return secondJoin.First.Compose(computation, senderPlacement, i => this.Exit(i, placement.Count));
                        }).SetCheckpointPolicy(s => new CheckpointWithoutPersistence());
                }

                if (Enumerable.Range(this.baseProc, this.range).Contains(computation.Controller.Configuration.ProcessID))
                {
                    if (computation.Controller.Configuration.ProcessID == this.baseProc)
                    {
                        computation.OnStageStable += this.ReactToStable;
                    }
                    else
                    {
                        this.dataSource.OnCompleted();
                    }
                }
            }

            public FastPipeline(int baseProc, int range)
            {
                this.baseProc = baseProc;
                this.range = range;
            }
        }

        public class CCPipeline
        {
            public int baseProc;
            public int range;
            public SubBatchDataSource<HTRecord, IterationIn<Epoch>> source;

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

            private Stream<Record, IterationIn<IterationIn<Epoch>>> Reduce(Stream<HTRecord, IterationIn<IterationIn<Epoch>>> input)
            {
                var smaller = input.Select(r => new Record(r)).SetCheckpointPolicy(i => new CheckpointEagerly());
                var consumable = smaller.PartitionBy(r => r.key).SetCheckpointPolicy(i => new CheckpointEagerly());
                var reduced = consumable.SetCheckpointType(CheckpointType.StatelessLogAll).SetCheckpointPolicy(i => new CheckpointEagerly());
                this.reduceStage = reduced.ForStage.StageId;
                return reduced;
            }

            public Stream<Pair<long, long>, IterationIn<Epoch>> TimeWindow(Stream<Record, IterationIn<Epoch>> input, int workerCount)
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

            private Collection<Record, IterationIn<Epoch>> Compute(Collection<Record, IterationIn<Epoch>> input)
            {
                return input;
            }

            public Stream<R, IterationIn<T>> MakeInput<R, T>(
                Computation computation, Placement inputPlacement, SubBatchDataSource<R, T> source)
                where T : Time<T>
            {
                Stream<R, IterationIn<T>> input;

                using (var placement = computation.WithPlacement(inputPlacement))
                {
                    input = computation.NewInput(source).SetCheckpointPolicy(s => new CheckpointEagerly());
                }

                return input;
            }

            public int reduceStage;
            public IEnumerable<int> ToMonitor
            {
                get { return new int[] { reduceStage }; }
            }

            public void Make(Computation computation, Placement inputPlacement, SlowPipeline slow, FastPipeline buggy, FastPipeline perfect)
            {
                this.source = new SubBatchDataSource<HTRecord, IterationIn<Epoch>>();

                Placement ccPlacement =
                    new Placement.ProcessRange(Enumerable.Range(this.baseProc, this.range),
                        Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));

                var slowOutput = slow.Make(computation);

                Stream<Pair<long, long>, IterationIn<Epoch>> ccWindow;

                var forCC = computation.BatchedEntry<Record, Epoch>(c =>
                    {
                        Collection<Record, IterationIn<Epoch>> cc;

                        using (var p = computation.WithPlacement(ccPlacement))
                        {
                            var reduced = computation
                                .BatchedEntry<Record, IterationIn<Epoch>>(ic =>
                                    {
                                        var input = this.MakeInput(computation, inputPlacement, this.source);
                                        return this.Reduce(input);
                                    });

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

                            cc = this.Compute(asCollection);
                            
                            ccWindow = this.TimeWindow(reduced, ccPlacement.Count);
                        }

                        //buggy.Make(computation, c.EnterLoop(slowOutput.Output).AsCollection(false), cc);
                        perfect.Make(computation, c.EnterLoop(slowOutput.First.Output).AsCollection(false), c.EnterLoop(slowOutput.Second), cc, ccWindow);

                        return cc;
                    });
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

        private int processId;
        private FileStream checkpointLogFile = null;
        private StreamWriter checkpointLog = null;
        internal StreamWriter CheckpointLog
        {
            get
            {
                if (checkpointLog == null)
                {
                    string fileName = String.Format("fastPipe.{0:D3}.log", this.processId);
                    this.checkpointLogFile = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                    this.checkpointLog = new StreamWriter(this.checkpointLogFile);
                    var flush = new System.Threading.Thread(new System.Threading.ThreadStart(() => FlushThread()));
                    flush.Start();
                }
                return checkpointLog;
            }
        }

        private void FlushThread()
        {
            while (true)
            {
                Thread.Sleep(1000);
                lock (this)
                {
                    if (this.checkpointLog != null)
                    {
                        this.checkpointLog.Flush();
                        this.checkpointLogFile.Flush(true);
                    }
                }
            }
        }

        public void WriteLog(string entry)
        {
            lock (this)
            {
                this.CheckpointLog.WriteLine(entry);
            }
        }

        public class BatchMaker
        {
            private readonly int processId;
            private readonly int processes;

            // We ensure that each key has at least one edge present that is never removed, and those are introduced
            // using this method
            private IEnumerable<HTRecord> MakeStartingBatch(Random random)
            {
                for (int i = (this.processId % this.processes); i < Program.numberOfKeys; i += this.processes)
                {
                    yield return new HTRecord
                    {
                        key = i,
                        otherKey = random.Next(numberOfKeys),
                        entryTicks = 0
                    };
                }
            }

            // keep track of the times of batches we put in, so we can remove the exact same data later
            private readonly Queue<long> batchTimes = new Queue<long>();
            private Random introduceRandom;
            private Random removeRandom;

            private IEnumerable<HTRecord> MakeBatch(Random random, int batchSize, long entryTicks)
            {
                if (entryTicks >= 0)
                {
                    // the batch is being added, so save its time
                    this.batchTimes.Enqueue(entryTicks);
                }
                else
                {
                    // the batch is being removed, so look up the time that it was put in
                    entryTicks = -(this.batchTimes.Dequeue());
                }

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

            public IEnumerable<HTRecord> NextBatch(long entryTicks)
            {
                IEnumerable<HTRecord> batch;

                if (batchesReturned == 0)
                {
                    Random thisProcessRandom = new Random();
                    int randomSeed = thisProcessRandom.Next();

                    // make matching random number generators for adding and removing records
                    this.introduceRandom = new Random(randomSeed);
                    this.removeRandom = new Random(randomSeed);

                    // add all the records that are not going to get removed.
                    batch = this.MakeStartingBatch(thisProcessRandom);
                }
                else
                {
                    batch = this.MakeBatch(this.introduceRandom, Program.htBatchSize, entryTicks);
                    if (this.batchesReturned >= Program.htInitialBatches)
                    {
                        batch = batch.Concat(this.MakeBatch(this.removeRandom, Program.htBatchSize, -1));
                    }
                }

                ++this.batchesReturned;
                return batch.ToArray();
            }

            public BatchMaker(int processes, int processId)
            {
                this.processes = processes;
                this.processId = processId;
            }
        };

        private int currentCompletedSlowEpoch = -1;

        public void AcceptSlowStableTime(Pointstamp stamp)
        {
            IterationIn<Epoch> slowTime = new IterationIn<Epoch>(new Epoch(stamp.Timestamp.a), stamp.Timestamp.b);

            lock (this)
            {
                if (slowTime.outerTime.epoch != this.currentCompletedSlowEpoch)
                {
                    this.currentCompletedSlowEpoch = slowTime.outerTime.epoch;
                    Console.WriteLine("Slow stable epoch " + this.currentCompletedSlowEpoch);
                }
            }
        }

        public void AcceptCCReduceStableTime(Pointstamp stamp)
        {
            KeyValuePair<IterationIn<IterationIn<Epoch>>, long>[] earlier;
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
                Console.WriteLine("CC reduce " + ccBatch.Key + " " + ccBatch.Value + "->" + now + ": " + ((double)(now - ccBatch.Value) / (double)TimeSpan.TicksPerMillisecond));
            }
        }

        public void AcceptSlowReduceStableTime(Pointstamp stamp)
        {
            KeyValuePair<IterationIn<Epoch>, long>[] earlier;
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
                Console.WriteLine("Slow reduce " + slowBatch.Key + " " + slowBatch.Value + "->" + now + ": " + ((double)(now - slowBatch.Value) / (double)TimeSpan.TicksPerMillisecond));
            }
        }

        void HighThroughputBatchInitiator()
        {
            long now = DateTime.Now.Ticks;
            long nextSlowBatch = now / TimeSpan.TicksPerMillisecond + Program.slowBatchTime;
            long nextCCBatch = now / TimeSpan.TicksPerMillisecond + Program.ccBatchTime;

            Epoch sendingSlowBatch = new Epoch(0);
            IterationIn<Epoch> sendingCCBatch = new IterationIn<Epoch>(new Epoch(0), 0);

            while (true)
            {
                now = DateTime.Now.Ticks;

                if (now / TimeSpan.TicksPerMillisecond > nextSlowBatch)
                {
                    sendingSlowBatch = new Epoch(sendingSlowBatch.epoch + 1);
                    nextSlowBatch += Program.slowBatchTime;
                }

                if (now / TimeSpan.TicksPerMillisecond > nextCCBatch)
                {
                    sendingCCBatch = new IterationIn<Epoch>(sendingCCBatch.outerTime, sendingCCBatch.iteration + 1);
                    nextCCBatch += Program.ccBatchTime;
                }

                lock (this)
                {
                    if (this.currentCompletedSlowEpoch > sendingCCBatch.outerTime.epoch)
                    {
                        sendingCCBatch = new IterationIn<Epoch>(new Epoch(this.currentCompletedSlowEpoch), 0);
                    }
                }

                Console.WriteLine("Sending slow " + sendingSlowBatch + " cc " + sendingCCBatch);

                // tell each input worker to start the next batch
                this.batchCoordinator.OnNext(Enumerable
                    .Range(Program.slowBase, Program.slowRange)
                    .Select(i => i.PairWith(now.PairWith(sendingSlowBatch.PairWith(sendingCCBatch)))));

                Thread.Sleep(Program.htSleepTime);
            }
        }

        private BatchMaker batchMaker;
        private Epoch currentSlowBatch = new Epoch(0);
        private IterationIn<Epoch> nextSlowInnerBatch = new IterationIn<Epoch>(new Epoch(0), 0);
        private IterationIn<Epoch> currentCCBatch = new IterationIn<Epoch>(new Epoch(0), 0);
        private IterationIn<IterationIn<Epoch>> nextCCInnerBatch = new IterationIn<IterationIn<Epoch>>(new IterationIn<Epoch>(new Epoch(0), 0), 0);

        private readonly Dictionary<IterationIn<Epoch>, long> slowBatchEntryTime = new Dictionary<IterationIn<Epoch>, long>();
        private readonly Dictionary<IterationIn<IterationIn<Epoch>>, long> ccBatchEntryTime = new Dictionary<IterationIn<IterationIn<Epoch>>, long>();

        void SendBatch(long entryTicks, Epoch slowBatch, IterationIn<Epoch> ccBatch)
        {
            var batch = this.batchMaker.NextBatch(entryTicks);

            // tell each slow worker to start the next batch
            if (!this.currentSlowBatch.Equals(slowBatch))
            {
                this.slow.source.CompleteOuterBatch(new Epoch(slowBatch.epoch - 1));
                this.currentSlowBatch = slowBatch;
                this.nextSlowInnerBatch = new IterationIn<Epoch>(this.currentSlowBatch, 0);
            }

            if (this.processId == Program.slowBase)
            {
                lock (this.slowBatchEntryTime)
                {
                    this.slowBatchEntryTime.Add(this.nextSlowInnerBatch, entryTicks);
                }
            }

            this.slow.source.OnNext(batch);
            this.slow.source.CompleteInnerBatch();
            ++this.nextSlowInnerBatch.iteration;
            
            // tell each CC worker to start the next batch
            if (!this.currentCCBatch.Equals(ccBatch))
            {
                if (ccBatch.iteration == 0)
                {
                    this.cc.source.CompleteOuterBatch(new IterationIn<Epoch>(new Epoch(ccBatch.outerTime.epoch - 1), int.MaxValue));
                }
                else
                {
                    this.cc.source.CompleteOuterBatch(new IterationIn<Epoch>(ccBatch.outerTime, ccBatch.iteration - 1));
                }
                this.currentCCBatch = ccBatch;
                this.nextCCInnerBatch = new IterationIn<IterationIn<Epoch>>(this.currentCCBatch, 0);
            }

            if (this.processId == Program.slowBase)
            {
                lock (this.ccBatchEntryTime)
                {
                    this.ccBatchEntryTime.Add(this.nextCCInnerBatch, entryTicks);
                }
            }

            this.cc.source.OnNext(batch);
            this.cc.source.CompleteInnerBatch();
            ++this.nextCCInnerBatch.iteration;
        }

        private void StartBatches()
        {
            var thread = new System.Threading.Thread(new System.Threading.ThreadStart(this.HighThroughputBatchInitiator));
            thread.Start();
        }

        private void ReactToStable(object o, StageStableEventArgs args)
        {
            if (args.stageId == this.perfect.slowStage)
            {
                this.AcceptSlowStableTime(args.frontier[0]);
            }
            else if (args.stageId == this.cc.reduceStage)
            {
                this.AcceptCCReduceStableTime(args.frontier[0]);
            }
            else if (args.stageId == this.slow.reduceStage)
            {
                this.AcceptSlowReduceStableTime(args.frontier[0]);
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
#if false
        static private int slowBase = 0;
        static private int slowRange = 1;
        static private int ccBase = 1;
        static private int ccRange = 1;
        static private int fbBase = 1;
        static private int fbRange = 1;
        static private int fpBase = 2;
        static private int fpRange = 1;
        static private int numberOfKeys = 100;
        static private int fastBatchSize = 1;
        static private int fastSleepTime = 100;
        static private int ccBatchTime = 1000;
        static private int slowBatchTime = 60000;
        static private int htBatchSize = 10;
        static private int htInitialBatches = 100;
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
        private int processes;
        private Computation computation;
        private SlowPipeline slow;
        private CCPipeline cc;
        private FastPipeline buggy;
        private FastPipeline perfect;
        private BatchedDataSource<Pair<int, Pair<long, Pair<Epoch, IterationIn<Epoch>>>>> batchCoordinator;

        public void Execute(string[] args)
        {
            FTManager manager = new FTManager();

            Configuration conf = Configuration.FromArgs(ref args);
            this.processId = conf.ProcessID;
            this.processes = conf.Processes;
            this.batchMaker = new BatchMaker(this.processes, this.processId);

            if (args.Length > 0 && args[0].ToLower() == "-azure")
            {
                conf.CheckpointingFactory = s => new AzureStreamSequence(accountName, accountKey, containerName, s);
            }
            else
            {
                System.IO.Directory.CreateDirectory("checkpoint");
                conf.CheckpointingFactory = s => new FileStreamSequence("checkpoint", s);
            }

            conf.DefaultCheckpointInterval = 50000;

            using (var computation = NewComputation.FromConfig(conf))
            {
                this.computation = computation;
                this.slow = new SlowPipeline(slowBase, slowRange);
                this.cc = new CCPipeline(ccBase, ccRange);
                this.buggy = new FastPipeline(fbBase, fbRange);
                this.perfect = new FastPipeline(fpBase, fpRange);

                Placement inputPlacement = new Placement.ProcessRange(Enumerable.Range(slowBase, slowRange), Enumerable.Range(0, 1));

                this.batchCoordinator = new BatchedDataSource<Pair<int, Pair<long, Pair<Epoch, IterationIn<Epoch>>>>>();
                using (var p = computation.WithPlacement(inputPlacement))
                {
                    computation.NewInput(this.batchCoordinator).SetCheckpointType(CheckpointType.None)
                        .PartitionBy(x => x.First).SetCheckpointType(CheckpointType.None)
                        .PartitionedActionStage(x => this.SendBatch(x.First, x.Second.First, x.Second.Second));
                }

                this.cc.Make(computation, inputPlacement, this.slow, this.buggy, this.perfect);

                if (conf.ProcessID == 0)
                {
                    manager.Initialize(computation, this.slow.ToMonitor.Concat(this.cc.ToMonitor.Concat(this.perfect.ToMonitor.Concat(this.buggy.ToMonitor))).Distinct());
                }

                //computation.OnStageStable += (x, y) => { Console.WriteLine(y.stageId + " " + y.frontier[0]); };

                computation.Activate();

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                //computation.OnFrontierChange += (x, y) => { Console.WriteLine(stopwatch.Elapsed + "\t" + string.Join(", ", y.NewFrontier)); Console.Out.Flush(); };

                if (Enumerable.Range(slowBase, slowRange).Contains(conf.ProcessID))
                {
                    if (conf.ProcessID == slowBase)
                    {
                        computation.OnStageStable += this.ReactToStable;
                        this.StartBatches();
                    }
                    else
                    {
                        this.batchCoordinator.OnCompleted();
                    }
                }

                if (conf.ProcessID == 0)
                {
                    IEnumerable<int> failSlow = Enumerable.Range(slowBase, slowRange);
                    IEnumerable<int> failMedium =
                        Enumerable.Range(ccBase, ccRange).Concat(Enumerable.Range(fbBase, fbRange)).Distinct()
                        .Except(failSlow);
                    IEnumerable<int> failFast = Enumerable.Range(fpBase, fpRange)
                        .Except(failSlow.Concat(failMedium));

                    while (true)
                    {
                        System.Threading.Thread.Sleep(Timeout.Infinite);
                        System.Threading.Thread.Sleep(60000);
                        if (conf.Processes > 2)
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
