import Distributed
import DistributedCluster

public protocol VirtualActor: DistributedActor, Codable {
///  associatedtype Dependency: VirtualActorDependency
  static func spawn(on actorSystem: ClusterSystem, dependency: VirtualActorDependency) async throws -> Self
}

/// Unfortunately due to (associated type error)[https://github.com/swiftlang/swift/issues/74769] we can't just make it associated type
/// to VirtualActor protocol and then use it with distributed actors
public protocol VirtualActorDependency: Codable, Sendable {}
public enum VirtualActorError: Error, Codable, Sendable {
  case spawnDependencyTypeMismatch
}

public struct None: VirtualActorDependency {}
