import Distributed
import DistributedCluster

// Internal singleton to handle nodes
distributed actor VirtualNodeRouter: LifecycleWatch, ClusterSingleton {

  public enum Error: Swift.Error, Codable {
    case noNodesAvailable
    case noActorsAvailable
  }

  // Hash ring of nodes. `Virtual` here is part of `VirtualActor` term.
  private var virtualNodes: HashRing<VirtualNode>
  // When cleaning up we need to associate actor id with virtual id
  private var idMapping: [ClusterSystem.ActorID: VirtualActorID] = [:]
  // In flight tasks for finding/spawning
  private var inFlightSpawning: [VirtualActorID: Task<any VirtualActor, Swift.Error>] = [:]
  // In flight tasks for cleaning
  private var inFlightCleaning: [ClusterSystem.ActorID: Task<Void, any Swift.Error>] = [:]
  // Receptionist listining task
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
        guard !Task.isCancelled else { return }
        await self.add(node: virtualNode)
      }
    }
  }

  private func add(node virtualNode: VirtualNode) async {
    self.virtualNodes.addNode(virtualNode)
    self.watchTermination(of: virtualNode)
    try? await virtualNode.updateTimeoutSettings(self.idleTimeoutSettings)
  }

  /// - Parameters:
  /// - id—external (not system) id of an actor.
  /// - dependency—only needed when spawning an actor.
  distributed func getActor<A: VirtualActor, D: Sendable & Codable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A {
    guard let node = self.virtualNodes.getNode(for: id.rawValue) else { throw Error.noNodesAvailable }

    let task = self.getTaskForActor(
      actorType: A.self,
      identifiedBy: id,
      on: node,
      dependency: dependency
    )

    let actor = try await task.value
    self.idMapping[actor.id] = id
    guard let typed = actor as? A else { throw VirtualNodeError.typeMismatch }
    return typed
  }

  private func getTaskForActor<A: VirtualActor, D: Sendable & Codable>(
    actorType: A.Type,
    identifiedBy id: VirtualActorID,
    on node: VirtualNode,
    dependency: D
  ) -> Task<any VirtualActor, Swift.Error> {
    if let task = self.inFlightSpawning[id] { return task }
    let task = Task<any VirtualActor, Swift.Error> {
      defer { self.inFlightSpawning[id] = nil }

      do {
        /// Try to get an actor by id
        self.actorSystem.log.info("Getting actor \(id) from \(node.id)")
        let actor: A = try await node.findActor(identifiedBy: id)
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
          return actor
        default:
          throw error
        }
      }
    }
    self.inFlightSpawning[id] = task
    return task
  }

  distributed func cleanActor(identifiedBy id: ClusterSystem.ActorID) async throws {
    guard let virtualId = self.idMapping[id] else { return }

    if let task = self.inFlightCleaning[id] {
      return try await task.value
    }
    let task = Task<Void, any Swift.Error> {
      defer {
        self.inFlightCleaning[id] = nil
      }
      self.actorSystem.log.debug("Marking \(id) as inactive, virtualId: \(virtualId)")
      try await self.virtualNodes
        .getNode(for: virtualId.rawValue)?
        .removeActor(identifiedBy: virtualId)

      // FIXME: What if distributed removeActor fails?
      self.idMapping[id] = nil
    }
    self.inFlightCleaning[id] = task
    return try await task.value
  }

  init(
    actorSystem: ClusterSystem,
    replicationFactor: UInt64,
    idleTimeoutSettings: VirtualNode.IdleTimeoutSettings
  ) async {
    self.actorSystem = actorSystem
    self.virtualNodes = .init(virtualNodesCount: UInt64(replicationFactor))
    self.idleTimeoutSettings = idleTimeoutSettings
    for virtualNode in await self.actorSystem.receptionist.lookup(VirtualNode.key) {
      await self.add(node: virtualNode)
    }
    self.findVirtualNodes()
  }
}
