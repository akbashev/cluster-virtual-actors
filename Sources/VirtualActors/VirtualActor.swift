import Distributed
import DistributedCluster

public protocol VirtualActor: DistributedActor, Codable where ActorSystem == ClusterSystem {
  /// Unfortunately due to (associated type error)[https://github.com/swiftlang/swift/issues/74769] we can't just make it associated type
  ///  associatedtype Dependency: VirtualActorDependency
  static func spawn(on actorSystem: ClusterSystem, dependency: any Sendable & Codable) async throws -> Self
}

public enum VirtualActorError: Error, Codable, Sendable {
  case spawnDependencyTypeMismatch
}

extension VirtualActor {
  nonisolated func resign() async throws {
    try await self.actorSystem.virtualActors.cleanActor(self)
  }
}
