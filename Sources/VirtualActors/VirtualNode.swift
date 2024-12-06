import Distributed
import DistributedCluster

distributed public actor VirtualNode: Routable {
  
  nonisolated var address: Cluster.Node { self.actorSystem.cluster.node }
  
  private var virtualActors: [VirtualActorID: any VirtualActor] = [:]
  
  distributed public func findActor<A: VirtualActor>(identifiedBy id: VirtualActorID) async throws -> A {
    guard let actor = self.virtualActors[id] else { throw VirtualNodeError.actorIsMissing }
    guard let actor = actor as? A else { throw VirtualNodeError.typeMismatch }
    return actor
  }
  
  distributed func spawn<A: VirtualActor, D: VirtualActorDependency>(identifiedBy id: VirtualActorID, dependency: D) async throws -> A {
    let actor = try await A.spawn(on: self.actorSystem, dependency: dependency)
    self.virtualActors[id] = actor
    return actor
  }
  
//  /// For future use—we should manage lifecycle of an virtual actor somehow.
//  distributed public func closeActor(
//    identifiedBy id: ClusterSystem.ActorID
//  ) async {
//    let value = self.virtualActors.first(where: { $0.value.id == id })
//    if let virtualId = value?.key {
//      self.virtualActors.removeValue(forKey: virtualId)
//    }
//  }
//  
//  distributed public func removeAll() {
//    self.virtualActors.removeAll()
//  }

  public init(
    actorSystem: ClusterSystem
  ) async {
    self.actorSystem = actorSystem
    await actorSystem
      .receptionist
      .checkIn(self, with: Self.key)
  }
}

public enum VirtualNodeError: Error, Codable {
  case actorIsMissing
  case typeMismatch
}

extension VirtualNode {
  static var key: DistributedReception.Key<VirtualNode> { "virtual_node_distributed_key" }
}
