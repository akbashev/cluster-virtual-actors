import Distributed
import DistributedCluster

// Internal singleton to handle nodes
distributed actor VirtualNodeRouter: LifecycleWatch, ClusterSingleton {

  public enum Error: Swift.Error, Codable {
    case noNodesAvailable
    case noActorsAvailable
  }

  private var virtualNodes: HashRing<VirtualNode>
  private var actorIdToVirtualId: [ClusterSystem.ActorID: VirtualActorID] = [:]
  private var listeningTask: Task<Void, Never>?
  private let idleTimeoutSettings: VirtualNode.IdleTimeoutSettings

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
        try? await virtualNode.updateTimeoutSettings(self.idleTimeoutSettings)
      }
    }
  }

  /// - Parameters:
  /// - id—external (not system) id of an actor.
  /// - dependency—only needed when spawning an actor.
  distributed func getActor<A: VirtualActor, D: Sendable & Codable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A {
    guard let node = self.virtualNodes.getNode(for: id.rawValue) else { throw Error.noNodesAvailable }
    do {
      /// Try to get an actor by id
      self.actorSystem.log.info("Getting actor \(id) from \(node.id)")
      let actor: A = try await node.findActor(identifiedBy: id)
      if self.idleTimeoutSettings.isEnabled {
        self.actorIdToVirtualId[actor.id] = id
      }
      return actor
    } catch {
      switch error {
      /// If there are no actors available—let's try to build it
      case VirtualNodeError.actorIsMissing:
        /// Register actor on this node (for future lookups)
        self.actorSystem.log.info("Registered actor \(id) on \(node.id)")
        let actor: A = try await node.spawnActor(
          identifiedBy: id,
          dependency: dependency
        )
        if self.idleTimeoutSettings.isEnabled {
          self.actorIdToVirtualId[actor.id] = id
        }
        return actor
      default:
        throw error
      }
    }
  }

  distributed func markAsActive<A: VirtualActor>(actor: A) async {
    guard
      self.idleTimeoutSettings.isEnabled,
      let virtualId = self.actorIdToVirtualId[actor.id]
    else { return }
    try? await self.virtualNodes.getNode(for: virtualId.rawValue)?.markActorAsActive(identifiedBy: virtualId)
  }

  distributed func cleanActor(identifiedBy id: ClusterSystem.ActorID) {
    guard self.idleTimeoutSettings.isEnabled else { return }
    self.actorIdToVirtualId.removeValue(forKey: id)
  }

  init(
    actorSystem: ClusterSystem,
    replicationFactor: UInt64,
    idleTimeoutSettings: VirtualNode.IdleTimeoutSettings
  ) async {
    self.actorSystem = actorSystem
    self.virtualNodes = .init(virtualNodesCount: UInt64(replicationFactor))
    self.idleTimeoutSettings = idleTimeoutSettings
    self.findVirtualNodes()
  }
}
