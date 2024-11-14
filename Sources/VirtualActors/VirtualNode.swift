import Distributed
import DistributedCluster

distributed public actor VirtualNode {
  
  public enum Error: Swift.Error {
    case noActorAvailable
  }
  
  private lazy var virtualActors: [VirtualActorID: any VirtualActor] = [:]

  distributed func register<A: VirtualActor>(actor: A, with id: VirtualActorID) {
    self.virtualActors[id] = actor
  }
  
  distributed public func find<A: VirtualActor>(id: VirtualActorID) async throws -> A {
    guard let actor = self.virtualActors[id] as? A else {
      throw Error.noActorAvailable
    }
    return actor
  }
  
  distributed public func close(
    with id: ClusterSystem.ActorID
  ) async {
    let value = self.virtualActors.first(where: { $0.value.id == id })
    if let virtualId = value?.key {
      self.virtualActors.removeValue(forKey: virtualId)
    }
  }
  
  distributed public func removeAll() {
    self.virtualActors.removeAll()
  }

  public init(
    actorSystem: ClusterSystem
  ) async {
    self.actorSystem = actorSystem
    await actorSystem
      .receptionist
      .checkIn(self, with: Self.key)
  }
}

extension VirtualNode {
  static var key: DistributedReception.Key<VirtualNode> { "virtual_node_distributed_key" }
}
