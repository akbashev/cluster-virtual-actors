import Distributed
import DistributedCluster

typealias DefaultDistributedActorSystem = ClusterSystem

// Internal singleton to handle nodes
distributed actor VirtualNodeRouter: LifecycleWatch, ClusterSingleton {
  public enum Error: Swift.Error {
    case noNodesAvailable
    case noActorsAvailable
  }
  
  private var virtualNodes: HashRing<VirtualNode>
  private var listeningTask: Task<Void, Never>?

  func terminated(actor id: ActorID) async {
    for node in virtualNodes.nodes {
      if node.id == id {
        virtualNodes.removeNode(node)
      }
    }
  }
  
  func findVirtualNodes() {
    guard self.listeningTask == nil else {
      actorSystem.log.info("Already looking for nodes")
      return
    }
    
    self.listeningTask = Task {
      for await virtualNode in await actorSystem.receptionist.listing(of: VirtualNode.key) {
        self.virtualNodes.addNode(virtualNode)
        self.watchTermination(of: virtualNode)
      }
    }
  }
  
  /// - Parameters:
  /// - id—external (not system) id of an actor.
  /// - dependency—only needed when spawning an actor.
  distributed func getActor<A: VirtualActor>(withId id: VirtualActorID) async throws -> A {
    try await self.getNode(forId: id)
      .find(id: id)
  }
  
  
  /// - Parameters:
  /// - id—external (not system) id of an actor.
  /// - dependency—only needed when spawning an actor.
  distributed func getNode(forId id: VirtualActorID) async throws -> VirtualNode {
    guard let node = self.virtualNodes.getNode(for: id) else {
      // There should be always a node (at least local node), if not—something sus
      throw Error.noNodesAvailable
    }
    return node
  }
  
  /// Actors should be cleaned automatically, but for now unfortunately manual cleaning.
  distributed func close(
    with id: ClusterSystem.ActorID
  ) async {
    /// Just going through all nodes as ActorID != VirtualID
    for virtualNode in self.virtualNodes.nodes {
      try? await virtualNode.close(with: id)
    }
  }
  
  /// - Parameters:
  ///  - spawn—definining how an actor should be created.
  ///  Local node is created while initialising a factory.
  init(
    actorSystem: ClusterSystem,
    replicationFactor: Int
  ) async {
    self.actorSystem = actorSystem
    self.virtualNodes = .init(virtualNodes: replicationFactor)
    self.findVirtualNodes()
  }
}
