import Distributed
import DistributedCluster

public protocol VirtualActor: DistributedActor, Codable where ActorSystem == ClusterSystem {
  associatedtype Dependency: Codable & Sendable
  static func spawn(on actorSystem: ClusterSystem, dependency: Dependency) async throws -> Self
}

public enum VirtualActorError: Error, Codable, Sendable {
  case spawnDependencyTypeMismatch
}
