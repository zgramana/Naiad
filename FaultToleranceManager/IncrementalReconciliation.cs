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

using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.FaultToleranceManager
{
    internal struct SV : IEquatable<SV>
    {
        public int denseId;
        public int DenseStageId { get { return denseId >> 16; } }
        public int StageId(FTManager manager)
        {
            return manager.DenseStages[DenseStageId].StageId;
        }
        public int VertexId { get { return denseId & 0xffff; } }

        public SV(int Stage, int Vertex)
        {
            this.denseId = (Stage << 16) + Vertex;
        }

        public bool Equals(SV other)
        {
            return this.denseId == other.denseId;
        }

        public override int GetHashCode()
        {
            return this.denseId;
        }

        public override string ToString()
        {
            return this.DenseStageId + "." + this.VertexId;
        }
    }

    internal struct Edge : IEquatable<Edge>
    {
        public SV src;
        public SV dst;

        public bool Equals(Edge other)
        {
            return this.src.denseId == other.src.denseId && this.dst.denseId == other.dst.denseId;
        }

        public override int GetHashCode()
        {
            return src.GetHashCode() + 123412324 * dst.GetHashCode();
        }
    }

    internal struct LexStamp : IEquatable<LexStamp>, IComparable<LexStamp>
    {
        private int value;
        private const int ABITS = 15;
        private const int AMAX = (1 << ABITS) - 1;
        private const int BBITS = 8;
        private const int BMAX = (1 << BBITS) - 1;
        private const int CBITS = 8;
        private const int CMAX = (1 << CBITS) - 1;
        private const int AMASK = AMAX << (BBITS + CBITS);
        private const int BMASK = BMAX << CBITS;
        private const int CMASK = CMAX;
        private int a { get { return value >> (BBITS + CBITS); } }
        private int b { get { return (value & BMASK) >> CBITS; } }
        private int c { get { return value & CMASK; } }

        private void IncrementA(int inc)
        {
            value += (inc << (BBITS + CBITS));
        }
        private void DecrementA(int dec)
        {
            value -= (dec << (BBITS + CBITS));
        }
        private void SaturateA()
        {
            value |= AMASK;
        }
        private void ClearA()
        {
            value &= (~AMASK);
        }
        private void IncrementB(int inc)
        {
            value += (inc << CBITS);
        }
        private void DecrementB(int dec)
        {
            value -= (dec << CBITS);
        }
        private void SaturateB()
        {
            value |= BMASK;
        }
        private void ClearB()
        {
            value &= (~BMASK);
        }
        private void IncrementC(int inc)
        {
            value += inc;
        }
        private void DecrementC(int dec)
        {
            value -= dec;
        }
        private void SaturateC()
        {
            value |= CMASK;
        }
        private void ClearC()
        {
            value &= (~CMASK);
        }

        public LexStamp(LexStamp other)
        {
            value = other.value;
        }

        private const int CLIPSLOP = 2;

        private static int Clip(int val, int Max)
        {
            if (val >= Max - CLIPSLOP)
            {
                int fromTop = Int32.MaxValue - val;
                if (fromTop > CLIPSLOP)
                {
                    throw new ApplicationException("Coordinate out of range " + val + " : " + Max);
                }
                return Max - fromTop;
            }
            return val;
        }

        private static int ExpandClip(int val, int Max)
        {
            if (val >= Max - CLIPSLOP)
            {
                int fromTop = Max - val;
                return Int32.MaxValue - fromTop;
            }
            return val;
        }

        public LexStamp(Pointstamp stamp)
        {
            if (stamp.Timestamp.Length == 1)
            {
                int aVal = Clip(stamp.Timestamp[0], AMAX);
                value = aVal << (BBITS + CBITS);
            }
            else if (stamp.Timestamp.Length == 2)
            {
                int aVal = Clip(stamp.Timestamp[0], AMAX);
                int bVal = Clip(stamp.Timestamp[1], BMAX);
                value = (aVal << (BBITS + CBITS)) + (bVal << CBITS);
            }
            else if (stamp.Timestamp.Length == 3)
            {
                int aVal = Clip(stamp.Timestamp[0], AMAX);
                int bVal = Clip(stamp.Timestamp[1], BMAX);
                int cVal = Clip(stamp.Timestamp[2], CMAX);
                value = (aVal << (BBITS + CBITS)) + (bVal << CBITS) + cVal;
            }
            else
            {
                throw new ApplicationException("Bad stamp length " + stamp);
            }
        }

        public LexStamp(FTFrontier frontier)
        {
            if (frontier.Empty)
            {
                value = -1;
            }
            else if (frontier.Complete)
            {
                value = Int32.MaxValue;
            }
            else
            {
                LexStamp stamp = new LexStamp(frontier.maximalElement);
                value = stamp.value;
            }
        }

        public bool Empty { get { return value < 0; } }
        public bool Complete { get { return value == Int32.MaxValue; } }

        private void IncrementLexicographically(int length)
        {
            if (length == 1)
            {
                // maxvalue indicates that all the times in this coordinate are already present in the set
                if (a != AMAX)
                {
                    IncrementA(1);
                }
            }
            else if (length == 2)
            {
                // maxvalue indicates that all the times in this coordinate are already present in the set
                if (b != BMAX)
                {
                    IncrementB(1);
                }
            }
            else // length == 3
            {
                // maxvalue indicates that all the times in this coordinate are already present in the set
                if (c != CMAX)
                {
                    IncrementC(1);
                }
            }
        }

        private void MaybeDecrementA()
        {
            int aVal = a;
            if (aVal > 0)
            {
                if (aVal != AMAX)
                {
                    DecrementA(1);
                }
            }
            else
            {
                value = -1;
            }
        }

        private void MaybeDecrementB()
        {
            int bVal = b;
            if (bVal > 0)
            {
                if (bVal != BMAX)
                {
                    DecrementB(1);
                }
            }
            else
            {
                SaturateB();
                MaybeDecrementA();
            }
        }

        private void MaybeDecrementC()
        {
            int cVal = c;
            if (cVal > 0)
            {
                if (cVal != CMAX)
                {
                    DecrementC(1);
                }
            }
            else
            {
                SaturateC();
                MaybeDecrementB();
            }
        }

        private void DecrementLexicographically(int length)
        {
            if (length == 1)
            {
                MaybeDecrementA();
            }
            else if (length == 2)
            {
                MaybeDecrementB();
            }
            else // length == 3
            {
                MaybeDecrementC();
            }
        }

        public static LexStamp SetBelow(LexStamp other, int length)
        {
            LexStamp copy = new LexStamp(other);
            copy.DecrementLexicographically(length);
            return copy;
        }

        public LexStamp ProjectIteration(int length)
        {
            LexStamp copy = new LexStamp(this);

            if (!(this.Empty || this.Complete))
            {
                copy.IncrementLexicographically(length);
            }

            return copy;
        }

        public LexStamp ProjectIngress(int length)
        {
            LexStamp copy = new LexStamp(this);

            if (!(this.Empty || this.Complete))
            {
                if (length == 1)
                {
                    copy.SaturateB();
                }
                else if (length == 2)
                {
                    copy.SaturateC();
                }
                else // length == 3
                {
                    throw new ApplicationException("Ingressing from wrong LexStamp " + this + " " + length);
                }
            }

            return copy;
        }

        public LexStamp ProjectEgress(int length)
        {
            LexStamp copy = new LexStamp(this);

            if (!(this.Empty || this.Complete))
            {
                if (length == 1)
                {
                    throw new ApplicationException("Logic bug in projection");
                }
                else if (length == 2)
                {
                    copy.ClearB();
                    if (this.b != BMAX)
                    {
                        copy.DecrementLexicographically(1);
                    }
                }
                else // length == 3
                {
                    copy.ClearC();
                    if (this.c != CMAX)
                    {
                        copy.DecrementLexicographically(2);
                    }
                }
            }

            return copy;
        }

        public LexStamp Project(Dataflow.Stage stage, int length)
        {
            if (stage.IsIterationAdvance)
            {
                return this.ProjectIteration(length);
            }
            else if (stage.IsIngress)
            {
                return this.ProjectIngress(length);
            }
            else if (stage.IsEgress)
            {
                return this.ProjectEgress(length);
            }
            else
            {
                return this;
            }
        }

        public Pointstamp Time(int stageId, int length)
        {
            if (length == 1)
            {
                Pointstamp.FakeArray time = new Pointstamp.FakeArray(1);
                time.a = ExpandClip(a, AMAX);
                return new Pointstamp { Location = stageId, Timestamp = time };
            }
            else if (length == 2)
            {
                Pointstamp.FakeArray time = new Pointstamp.FakeArray(2);
                time.a = ExpandClip(a, AMAX);
                time.b = ExpandClip(b, BMAX);
                return new Pointstamp { Location = stageId, Timestamp = time };
            }
            else // length == 3
            {
                Pointstamp.FakeArray time = new Pointstamp.FakeArray(3);
                time.a = ExpandClip(a, AMAX);
                time.b = ExpandClip(b, BMAX);
                time.c = ExpandClip(c, CMAX);
                return new Pointstamp { Location = stageId, Timestamp = time };
            }
        }

        public bool Equals(LexStamp other)
        {
            return this.value == other.value;
        }

        public bool Contains(LexStamp stamp)
        {
            return stamp.value <= this.value;
        }

        public int CompareTo(LexStamp other)
        {
            if (this.value < other.value)
            {
                return -1;
            }
            else if (this.value > other.value)
            {
                return 1;
            }
            return 0;
        }

        public override int GetHashCode()
        {
            return value;
        }

        public override string ToString()
        {
            return "[" + this.a + "," + this.b + "," + this.c + "]";
        }
    }

    internal struct DeliveredMessage : IEquatable<DeliveredMessage>
    {
        public int srcDenseStage;
        public SV dst;
        public LexStamp dstTime;

        public bool Equals(DeliveredMessage other)
        {
            return
                dst.denseId == other.dst.denseId && srcDenseStage == other.srcDenseStage && dstTime.Equals(other.dstTime);
        }

        public override int GetHashCode()
        {
            return srcDenseStage + 12436432 * dst.GetHashCode() + 981225 * dstTime.GetHashCode();
        }
    }

    internal struct DiscardedMessage : IEquatable<DiscardedMessage>
    {
        public SV src;
        public int dstDenseStage;
        public LexStamp srcTime;
        public LexStamp dstTime;

        public bool Equals(DiscardedMessage other)
        {
            return
                src.denseId == other.src.denseId && dstDenseStage == other.dstDenseStage && srcTime.Equals(other.srcTime) && dstTime.Equals(other.dstTime);
        }

        public override int GetHashCode()
        {
            return src.GetHashCode() + 12436432 * dstDenseStage + 94323 * srcTime.GetHashCode() + 981225 * dstTime.GetHashCode();
        }
    }

    internal struct Notification : IEquatable<Notification>
    {
        public SV node;
        public LexStamp time;

        public bool Equals(Notification other)
        {
            return
                node.denseId == other.node.denseId && time.Equals(other.time);
        }

        public override int GetHashCode()
        {
            return node.denseId + 12436432 * time.GetHashCode();
        }
    }

    internal struct Frontier : IEquatable<Frontier>
    {
        public SV node;
        public LexStamp frontier;
        public bool isNotification;

        public Frontier(SV node, LexStamp frontier, bool isNotification)
        {
            this.node = node;
            this.frontier = frontier;
            this.isNotification = isNotification;
        }

        public Frontier(SV node, FTFrontier frontier, bool isNotification)
        {
            this.node = node;
            this.frontier = new LexStamp(frontier);
            this.isNotification = isNotification;
        }

        public FTFrontier ToFrontier(FTManager manager)
        {
            if (frontier.Empty)
            {
                return new FTFrontier(false);
            }
            if (frontier.Complete)
            {
                return new FTFrontier(true);
            }
            FTFrontier f = new FTFrontier();
            f.maximalElement = frontier.Time(
                node.StageId(manager),
                manager.DenseStages[node.DenseStageId].DefaultVersion.Timestamp.Length);
            return f;
        }

        public bool Equals(Frontier other)
        {
            return this.node.denseId == other.node.denseId
                && this.frontier.Equals(other.frontier)
                && this.isNotification == other.isNotification;
        }

        public override int GetHashCode()
        {
            return node.denseId + 12436432 * frontier.GetHashCode() + ((isNotification) ? 643 : 928);
        }
    }

    internal struct Checkpoint : IEquatable<Checkpoint>
    {
        public SV node;
        public LexStamp checkpoint;
        public bool downwardClosed;

        public Checkpoint(SV node, LexStamp checkpoint, bool downwardClosed)
        {
            this.node = node;
            this.checkpoint = checkpoint;
            this.downwardClosed = downwardClosed;
        }

        public Checkpoint(SV node, FTFrontier checkpoint, bool downwardClosed)
        {
            this.node = node;
            this.checkpoint = new LexStamp(checkpoint);
            this.downwardClosed = downwardClosed;
        }

        public bool Equals(Checkpoint other)
        {
            return
                node.denseId == other.node.denseId && checkpoint.Equals(other.checkpoint) && downwardClosed == other.downwardClosed;
        }

        public override int GetHashCode()
        {
            return node.denseId + 12436432 * checkpoint.GetHashCode() + ((downwardClosed) ? 643 : 928);
        }
    }

    internal static class ExtensionMethods
    {
        private static Pair<Checkpoint, LexStamp> PairCheckpointToBeLowerThanTime(Checkpoint checkpoint, LexStamp time, FTManager manager)
        {
            if (checkpoint.downwardClosed && checkpoint.checkpoint.Contains(time))
            {
                return new Checkpoint(
                    checkpoint.node,
                    LexStamp.SetBelow(
                        time,
                        manager.DenseStages[checkpoint.node.DenseStageId].DefaultVersion.Timestamp.Length),
                    true).PairWith(time);
            }
            else
            {
                return checkpoint.PairWith(time);
            }
        }

        private static Pair<Checkpoint, LexStamp> PairCheckpointToBeWithinFrontier(Checkpoint checkpoint, LexStamp frontier)
        {
            if (checkpoint.downwardClosed && checkpoint.checkpoint.Contains(frontier))
            {
                return new Checkpoint(checkpoint.node, frontier, true).PairWith(frontier);
            }
            else
            {
                return checkpoint.PairWith(frontier);
            }
        }

        public static Collection<Frontier, T> ReduceForDiscarded<T>(
            this Collection<Frontier, T> frontiers,
            Collection<Checkpoint, T> checkpoints,
            Collection<DiscardedMessage, T> discardedMessages,
            FTManager manager) where T : Time<T>
        {
            // We only need the lowest send time along an edge for each destination time.
            var prunedDiscarded = discardedMessages
                .Min(m => m.dstDenseStage.PairWith(m.src).PairWith(m.dstTime), m => m.srcTime);

            return frontiers
                // only take the restoration frontiers
                .Where(f => !f.isNotification)
                // only take the lowest frontier at each stage
                .Min(f => f.node.DenseStageId, f => f.frontier)
                // match with all the discarded messages to the node for a given restoration frontier
                .Join(prunedDiscarded, f => f.node.DenseStageId, m => m.dstDenseStage, (f, m) => f.PairWith(m))
                // keep all discarded messages that are outside the restoration frontier at the node
                .Where(p => !p.First.frontier.Contains(p.Second.dstTime))
                // we only need the sender node id and the send time of the discarded message
                .Select(p => p.Second.src.PairWith(p.Second.srcTime))
                // keep the sender node and minimum send time of any discarded message outside its destination restoration frontier
                .Min(m => m.First, m => m.Second)
                // for each node that sent a needed discarded message, match it up with all the available checkpoints,
                // reducing downward-closed checkpoints to be less than the time the message was sent
                .Join(
                    checkpoints, m => m.First, c => c.node,
                    (m, c) => PairCheckpointToBeLowerThanTime(c, m.Second, manager))
                // then throw out any checkpoints that included any required but discarded sent messages
                .Where(c => !c.First.checkpoint.Contains(c.Second))
                // and just keep the feasible checkpoint
                .Select(c => c.First)
                // now select the largest feasible checkpoint at each node constrained by discarded messages
                .Max(c => c.node, c => c.checkpoint)
                // and convert it to a pair of frontiers
                .SelectMany(c => new Frontier[] {
                    new Frontier(c.node, c.checkpoint, false),
                    new Frontier(c.node, c.checkpoint, true) });
        }

        public static Collection<Frontier, T> Reduce<T>(
            this Collection<Frontier, T> frontiers,
            Collection<Checkpoint, T> checkpoints,
            Collection<DeliveredMessage, T> deliveredMessageTimes,
            Collection<Notification, T> deliveredNotificationTimes,
            Collection<Edge, T> graph, FTManager manager) where T : Time<T>
        {
            Collection<Pair<Pair<int, SV>, LexStamp>, T> projectedMessageFrontiers = frontiers
                // only look at the restoration frontiers
                .Where(f => !f.isNotification)
                // project each frontier along each outgoing edge
                .Join(
                    graph, f => f.node, e => e.src,
                    (f, e) => e.src.DenseStageId
                        .PairWith(e.dst)
                        .PairWith(f.frontier.Project(
                            manager.DenseStages[e.src.DenseStageId],
                            manager.DenseStages[e.src.DenseStageId].DefaultVersion.Timestamp.Length)))
                // keep only the lowest projected frontier from each src stage
                .Min(f => f.First, f => f.Second);

            Collection<Pair<SV,LexStamp>,T> staleDeliveredMessages = deliveredMessageTimes
                // make sure messages are unique
                .Distinct()
                // match up delivered messages with the projected frontier along the delivery edge,
                // keeping the dst node, dst time and projected frontier
                .Join(
                    projectedMessageFrontiers, m => m.srcDenseStage.PairWith(m.dst), f => f.First,
                    (m, f) => f.First.Second.PairWith(m.dstTime.PairWith(f.Second)))
                // filter to keep only messages that fall outside their projected frontiers
                .Where(m => !m.Second.Second.Contains(m.Second.First))
                // we only care about the destination node and stale message time
                .Select(m => m.First.PairWith(m.Second.First));

            Collection<Frontier, T> intersectedProjectedNotificationFrontiers = frontiers
                // only look at the notification frontiers
                .Where(f => f.isNotification)
                // project each frontier along each outgoing edge to its destination
                .Join(
                    graph, f => f.node, e => e.src,
                    (f, e) => new Frontier(e.dst, f.frontier.Project(
                                                    manager.DenseStages[e.src.DenseStageId],
                                                    manager.DenseStages[e.src.DenseStageId].DefaultVersion.Timestamp.Length), true))
                // and find the intersection (minimum) of the projections at the destination
                .Min(f => f.node, f => f.frontier);

            Collection<Pair<SV,LexStamp>,T> staleDeliveredNotifications = deliveredNotificationTimes
                // match up delivered notifications with the intersected projected notification frontier at the node,
                // keeping node, time and intersected projected frontier
                .Join(
                    intersectedProjectedNotificationFrontiers, n => n.node, f => f.node,
                    (n, f) => n.node.PairWith(n.time.PairWith(f.frontier)))
                // filter to keep only notifications that fall outside their projected frontiers
                .Where(n => !n.Second.Second.Contains(n.Second.First))
                // we only care about the node and stale notification time
                .Select(n => n.First.PairWith(n.Second.First));

            Collection<Pair<SV,LexStamp>,T> earliestStaleEvents = staleDeliveredMessages
                .Concat(staleDeliveredNotifications)
                // keep only the earliest stale event at each node
                .Min(n => n.First, n => n.Second);

            var reducedFrontiers = checkpoints
                // for each node that executed a stale, match it up with all the available checkpoints,
                // reducing downward-closed checkpoints to be less than the time the event happened at
                .Join(
                    earliestStaleEvents, c => c.node, e => e.First,
                    (c, e) => PairCheckpointToBeLowerThanTime(c, e.Second, manager))
                // then throw out any checkpoints that included any stale events
                .Where(c => !c.First.checkpoint.Contains(c.Second))
                // and select the largest feasible checkpoint at each node
                .Max(c => c.First.node, c => c.First.checkpoint)
                // then convert it to a pair of frontiers
                .SelectMany(c => new Frontier[] {
                    new Frontier(c.First.node, c.First.checkpoint, false),
                    new Frontier(c.First.node, c.First.checkpoint, true) });

            // return any reduction in either frontier
            return reducedFrontiers.Concat(intersectedProjectedNotificationFrontiers);
        }
    }
}
