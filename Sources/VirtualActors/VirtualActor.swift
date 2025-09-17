import Distributed
import DistributedCluster

public protocol VirtualActor: DistributedActor, Codable where ActorSystem == ClusterSystem {
  associatedtype Dependency: Codable & Sendable
  /// Unfortunately due to (associated type error)[https://github.com/swiftlang/swift/issues/74769] we can't just make it associated type
  ///  associatedtype Dependency: VirtualActorDependency
  static func spawn(on actorSystem: ClusterSystem, dependency: Dependency) async throws -> Self
}

public enum VirtualActorError: Error, Codable, Sendable {
  case spawnDependencyTypeMismatch
}
