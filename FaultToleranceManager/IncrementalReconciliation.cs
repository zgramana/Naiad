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
        private int a, b, c;

        public LexStamp(LexStamp other)
        {
            a = other.a;
            b = other.b;
            c = other.c;
        }

        public LexStamp(Pointstamp stamp)
        {
            if (stamp.Timestamp.Length == 1)
            {
                a = stamp.Timestamp[0];
                b = -1;
                c = -1;
            }
            else if (stamp.Timestamp.Length == 2)
            {
                a = stamp.Timestamp[0];
                b = stamp.Timestamp[1];
                c = -1;
            }
            else if (stamp.Timestamp.Length == 3)
            {
                a = stamp.Timestamp[0];
                b = stamp.Timestamp[1];
                c = stamp.Timestamp[2];
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
                a = -1;
                b = -1;
                c = -1;
            }
            else if (frontier.Complete)
            {
                a = Int32.MaxValue;
                b = Int32.MaxValue;
                c = Int32.MaxValue;
            }
            else
            {
                LexStamp stamp = new LexStamp(frontier.maximalElement);
                a = stamp.a;
                b = stamp.b;
                c = stamp.c;
            }
        }

        public bool Empty { get { return a < 0; } }
        public bool Complete { get { return a == Int32.MaxValue && b == Int32.MaxValue && c == Int32.MaxValue; } }

        private void IncrementLexicographically()
        {
            if (b < 0)
            {
                // length == 1
                // maxvalue indicates that all the times in this coordinate are already present in the set
                if (a != Int32.MaxValue)
                {
                    ++a;
                }
            }
            else if (c < 0)
            {
                // length == 2
                // maxvalue indicates that all the times in this coordinate are already present in the set
                if (b != Int32.MaxValue)
                {
                    ++b;
                }
            }
            else
            {
                // length == 3
                // maxvalue indicates that all the times in this coordinate are already present in the set
                if (c != Int32.MaxValue)
                {
                    ++c;
                }
            }
        }

        private void DecrementA()
        {
            if (a > 0)
            {
                if (a != Int32.MaxValue)
                {
                    --a;
                }
            }
            else
            {
                a = -1;
                b = -1;
                c = -1;
            }
        }

        private void DecrementB()
        {
            if (b > 0)
            {
                if (b != Int32.MaxValue)
                {
                    --b;
                }
            }
            else
            {
                b = Int32.MaxValue;
                DecrementA();
            }
        }

        private void DecrementC()
        {
            if (c > 0)
            {
                if (c != Int32.MaxValue)
                {
                    --c;
                }
            }
            else
            {
                c = Int32.MaxValue;
                DecrementB();
            }
        }

        private void DecrementLexicographically()
        {
            if (b < 0)
            {
                // length == 1
                DecrementA();
            }
            else if (c < 0)
            {
                // length = 2
                DecrementB();
            }
            else
            {
                // length = 3
                DecrementC();
            }
        }

        public static LexStamp SetBelow(LexStamp other)
        {
            LexStamp copy = new LexStamp(other);
            copy.DecrementLexicographically();
            return copy;
        }

        public LexStamp ProjectIteration()
        {
            LexStamp copy = new LexStamp(this);

            if (!(this.Empty || this.Complete))
            {
                copy.IncrementLexicographically();
            }

            return copy;
        }

        public LexStamp ProjectIngress()
        {
            LexStamp copy = new LexStamp(this);

            if (!(this.Empty || this.Complete))
            {
                if (b < 0)
                {
                    // length == 1
                    copy.b = Int32.MaxValue;
                }
                else if (c < 0)
                {
                    // length == 2
                    copy.c = Int32.MaxValue;
                }
                else
                {
                    // length == 3
                    throw new ApplicationException("Ingressing from wrong LexStamp " + this);
                }
            }

            return copy;
        }

        public LexStamp ProjectEgress()
        {
            LexStamp copy = new LexStamp(this);

            if (!(this.Empty || this.Complete))
            {
                if (this.b < 0)
                {
                    // length == 1
                    throw new ApplicationException("Logic bug in projection");
                }
                else if (this.c < 0)
                {
                    // length == 2
                    copy.b = -1;
                    if (this.b != Int32.MaxValue)
                    {
                        copy.DecrementLexicographically();
                    }
                }
                else
                {
                    // length = 3
                    copy.c = -1;
                    if (this.c != Int32.MaxValue)
                    {
                        copy.DecrementLexicographically();
                    }
                }
            }

            return copy;
        }

        public LexStamp Project(Dataflow.Stage stage)
        {
            if (stage.IsIterationAdvance)
            {
                return this.ProjectIteration();
            }
            else if (stage.IsIngress)
            {
                return this.ProjectIngress();
            }
            else if (stage.IsEgress)
            {
                return this.ProjectEgress();
            }
            else
            {
                return this;
            }
        }

        public Pointstamp Time(int stageId)
        {
            if (b < 0)
            {
                Pointstamp.FakeArray time = new Pointstamp.FakeArray(1);
                time.a = a;
                return new Pointstamp { Location = stageId, Timestamp = time };
            }
            else if (c < 0)
            {
                Pointstamp.FakeArray time = new Pointstamp.FakeArray(2);
                time.a = a;
                time.b = b;
                return new Pointstamp { Location = stageId, Timestamp = time };
            }
            else
            {
                Pointstamp.FakeArray time = new Pointstamp.FakeArray(3);
                time.a = a;
                time.b = b;
                time.c = c;
                return new Pointstamp { Location = stageId, Timestamp = time };
            }
        }

        public bool Equals(LexStamp other)
        {
            return this.a == other.a && this.b == other.b && this.c == other.c;
        }

        public bool Contains(LexStamp stamp)
        {
            return stamp.CompareTo(this) <= 0;
        }

        public int CompareTo(LexStamp other)
        {
            if (this.a < other.a)
            {
                return -1;
            }
            else if (this.a > other.a)
            {
                return 1;
            }
            if (this.b < other.b)
            {
                return -1;
            }
            else if (this.b > other.b)
            {
                return 1;
            }
            if (this.c < other.c)
            {
                return -1;
            }
            else if (this.c > other.c)
            {
                return 1;
            }
            return 0;
        }

        public override int GetHashCode()
        {
            return a + 84327 * b + 123412324 * c;
        }

        public override string ToString()
        {
            if (this.b < 0) return "[" + this.a + "]";
            if (this.c < 0) return "[" + this.a + "," + this.b + "]";
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
            f.maximalElement = frontier.Time(node.StageId(manager));
            return f;
        }

        public bool Equals(Frontier other)
        {
            return this.node.denseId == other.node.denseId
                && this.frontier.Equals(other.frontier)
                && this.isNotification.Equals(other.isNotification);
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
        private static Pair<Checkpoint, LexStamp> PairCheckpointToBeLowerThanTime(Checkpoint checkpoint, LexStamp time)
        {
            if (checkpoint.downwardClosed && checkpoint.checkpoint.Contains(time))
            {
                return new Checkpoint(checkpoint.node, LexStamp.SetBelow(time), true).PairWith(time);
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
            Collection<DiscardedMessage, T> discardedMessages) where T : Time<T>
        {
            return frontiers
                // only take the restoration frontiers
                .Where(f => !f.isNotification)
                // only take the lowest frontier at each stage
                .Min(f => f.node.DenseStageId, f => f.frontier)
                // match with all the discarded messages to the node for a given restoration frontier
                .Join(discardedMessages, f => f.node.DenseStageId, m => m.dstDenseStage, (f, m) => f.PairWith(m))
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
                    (m, c) => PairCheckpointToBeLowerThanTime(c, m.Second))
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
                        .PairWith(f.frontier.Project(manager.DenseStages[e.src.DenseStageId])))
                // keep only the lowest projected frontier from each src stage
                .Min(f => f.First, f => f.Second);

            Collection<Pair<SV,LexStamp>,T> staleDeliveredMessages = deliveredMessageTimes
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
                    (f, e) => new Frontier(e.dst, f.frontier.Project(manager.DenseStages[e.src.DenseStageId]), true))
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
                    (c, e) => PairCheckpointToBeLowerThanTime(c, e.Second))
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
