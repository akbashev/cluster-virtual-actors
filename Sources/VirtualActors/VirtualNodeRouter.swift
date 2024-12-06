import Distributed
import DistributedCluster

// Internal singleton to handle nodes
distributed actor VirtualNodeRouter: LifecycleWatch, ClusterSingleton {
  
  public enum Error: Swift.Error, Codable {
    case noNodesAvailable
    case noActorsAvailable
  }
  
  private var virtualNodes: HashRing<VirtualNode>
  private var listeningTask: Task<Void, Never>?

  func terminated(actor id: ActorID) async {
    for node in self.virtualNodes.nodes where node.id == id {
      self.virtualNodes.removeNode(node)
    }
  }
  
  private func findVirtualNodes() {
    guard self.listeningTask == nil else {
      self.actorSystem.log.info("Already looking for nodes")
      return
    }
    
    self.listeningTask = Task {
      for await virtualNode in await self.actorSystem.receptionist.listing(of: VirtualNode.key) {
        self.virtualNodes.addNode(virtualNode)
        self.watchTermination(of: virtualNode)
      }
    }
  }
  
  /// - Parameters:
  /// - id—external (not system) id of an actor.
  /// - dependency—only needed when spawning an actor.
  distributed func getNode(identifiedBy id: VirtualActorID) async throws -> VirtualNode {
    guard let node = self.virtualNodes.getNode(for: id) else { throw Error.noNodesAvailable }
    return node
  }
  
//  /// Actors should be cleaned automatically, but for now unfortunately manual cleaning.
//  distributed func closeActor(
//    identifiedBy id: ClusterSystem.ActorID
//  ) async {
//    /// Just going through all nodes as ActorID != VirtualID
//    for virtualNode in self.virtualNodes.nodes {
//      try? await virtualNode.closeActor(identifiedBy: id)
//    }
//  }
//  
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
