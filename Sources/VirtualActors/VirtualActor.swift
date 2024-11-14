import Distributed
import DistributedCluster

public typealias VirtualActorID = String

public protocol VirtualActor: DistributedActor, Codable where ActorSystem == ClusterSystem {
  // FIXME: Shouldn't be distributed?
  distributed var virtualID: VirtualActorID { get }
}
