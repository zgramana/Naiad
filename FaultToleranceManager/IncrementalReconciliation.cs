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
            return src.GetHashCode() + dst.GetHashCode();
        }
    }

    internal struct LexStamp : IEquatable<LexStamp>, IComparable<LexStamp>
    {
        private int a, b, c;

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
            return a + b + c;
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
    }

    internal struct Frontier : IEquatable<Frontier>
    {
        public SV node;
        public FTFrontier frontier;
        public bool isNotification;

        public Frontier(SV node, FTFrontier frontier, bool isNotification)
        {
            this.node = node;
            this.frontier = frontier;
            this.isNotification = isNotification;
        }

        public bool Equals(Frontier other)
        {
            return this.node.denseId == other.node.denseId
                && this.frontier.Equals(other.frontier)
                && this.isNotification.Equals(other.isNotification);
        }
    }

    internal struct Checkpoint : IEquatable<Checkpoint>
    {
        public SV node;
        public FTFrontier checkpoint;
        public bool downwardClosed;

        public bool Equals(Checkpoint other)
        {
            return
                node.denseId == other.node.denseId && checkpoint.Equals(other.checkpoint) && downwardClosed == other.downwardClosed;
        }
    }

    internal static class ExtensionMethods
    {
        private static Pair<Checkpoint, LexStamp> PairCheckpointToBeLowerThanTime(Checkpoint checkpoint, LexStamp time, FTManager manager)
        {
            Pointstamp stamp = time.Time(checkpoint.node.StageId(manager));
            if (checkpoint.downwardClosed && checkpoint.checkpoint.Contains(stamp))
            {
                return new Checkpoint
                {
                    node = checkpoint.node,
                    checkpoint = FTFrontier.SetBelow(stamp),
                    downwardClosed = true
                }.PairWith(time);
            }
            else
            {
                return checkpoint.PairWith(time);
            }
        }

        private static Pair<Checkpoint, FTFrontier> PairCheckpointToBeWithinFrontier(Checkpoint checkpoint, FTFrontier frontier)
        {
            if (checkpoint.downwardClosed && checkpoint.checkpoint.Contains(frontier))
            {
                return new Checkpoint
                {
                    node = checkpoint.node,
                    checkpoint = frontier,
                    downwardClosed = true
                }.PairWith(frontier);
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
            return frontiers
                // only take the restoration frontiers
                .Where(f => !f.isNotification)
                // match with all the discarded messages to the node for a given restoration frontier
                .Join(discardedMessages, f => f.node.DenseStageId, m => m.dstDenseStage, (f, m) => f.PairWith(m))
                // keep all discarded messages that are outside the restoration frontier at the node
                .Where(p => !p.First.frontier.Contains(p.Second.dstTime.Time(p.First.node.StageId(manager))))
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
                .Where(c => !c.First.checkpoint.Contains(c.Second.Time(c.First.node.StageId(manager))))
                // and just keep the feasible checkpoint
                .Select(c => c.First)
                // now select the largest feasible checkpoint at each node constrained by discarded messages
                .Max(c => c.node, c => c.checkpoint)
                // and convert it to a pair of frontiers
                .SelectMany(c => new Frontier[] {
                    new Frontier { node = c.node, frontier = c.checkpoint, isNotification = false },
                    new Frontier { node = c.node, frontier = c.checkpoint, isNotification = true } });
        }

        public static Collection<Frontier, T> Reduce<T>(
            this Collection<Frontier, T> frontiers,
            Collection<Checkpoint, T> checkpoints,
            Collection<DeliveredMessage, T> deliveredMessageTimes,
            Collection<Notification, T> deliveredNotificationTimes,
            Collection<Edge, T> graph, FTManager manager) where T : Time<T>
        {
            Collection<Pair<Pair<int, SV>, FTFrontier>, T> projectedMessageFrontiers = frontiers
                // only look at the restoration frontiers
                .Where(f => !f.isNotification)
                // project each frontier along each outgoing edge
                .Join(
                    graph, f => f.node, e => e.src,
                    (f, e) => e.src.DenseStageId
                        .PairWith(e.dst)
                        .PairWith(f.frontier.Project(manager.DenseStages[e.src.DenseStageId], e.dst.StageId(manager))));

            Collection<Frontier, T> intersectedProjectedNotificationFrontiers = frontiers
                // only look at the notification frontiers
                .Where(f => f.isNotification)
                // project each frontier along each outgoing edge to its destination
                .Join(
                    graph, f => f.node, e => e.src,
                    (f, e) => new Frontier {
                        node = e.dst,
                        frontier = f.frontier.Project(manager.DenseStages[e.src.DenseStageId], e.dst.StageId(manager)),
                        isNotification = true })
                // and find the intersection (minimum) of the projections at the destination
                .Min(f => f.node, f => f.frontier);

            Collection<Pair<SV,LexStamp>,T> staleDeliveredMessages = deliveredMessageTimes
                // match up delivered messages with the projected frontier along the delivery edge,
                // keeping the dst node, dst time and projected frontier
                .Join(
                    projectedMessageFrontiers, m => m.srcDenseStage.PairWith(m.dst), f => f.First,
                    (m, f) => f.First.Second.PairWith(m.dstTime.PairWith(f.Second)))
                // filter to keep only messages that fall outside their projected frontiers
                .Where(m => !m.Second.Second.Contains(m.Second.First.Time(m.First.StageId(manager))))
                // we only care about the destination node and stale message time
                .Select(m => m.First.PairWith(m.Second.First));

            Collection<Pair<SV,LexStamp>,T> staleDeliveredNotifications = deliveredNotificationTimes
                // match up delivered notifications with the intersected projected notification frontier at the node,
                // keeping node, time and intersected projected frontier
                .Join(
                    intersectedProjectedNotificationFrontiers, n => n.node, f => f.node,
                    (n, f) => n.node.PairWith(n.time.PairWith(f.frontier)))
                // filter to keep only notifications that fall outside their projected frontiers
                .Where(n => !n.Second.Second.Contains(n.Second.First.Time(n.First.StageId(manager))))
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
                .Where(c => !c.First.checkpoint.Contains(c.Second.Time(c.First.node.StageId(manager))))
                // and select the largest feasible checkpoint at each node
                .Max(c => c.First.node, c => c.First.checkpoint)
                // then convert it to a pair of frontiers
                .SelectMany(c => new Frontier[] {
                    new Frontier { node = c.First.node, frontier = c.First.checkpoint, isNotification = false },
                    new Frontier { node = c.First.node, frontier = c.First.checkpoint, isNotification = true } });

            // return any reduction in either frontier
            return reducedFrontiers.Concat(intersectedProjectedNotificationFrontiers);
        }
    }
}
