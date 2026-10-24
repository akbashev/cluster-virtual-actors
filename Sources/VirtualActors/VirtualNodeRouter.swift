import Distributed
import DistributedCluster

// Internal singleton to handle nodes
distributed actor VirtualNodeRouter: LifecycleWatch, ClusterSingleton {

  public enum Error: Swift.Error, Codable {
    case noNodesAvailable
    case noActorsAvailable
    case actorHostUnavailable
  }

  // Hash ring of nodes. `Virtual` here is part of `VirtualActor` term.
  private var virtualNodes: HashRing<VirtualNode>
  private struct Registration {
    let virtualID: VirtualActorID
    let hostID: ClusterSystem.ActorID
  }
  // The concrete actor identity is the lifecycle key. The value records the
  // logical identity and the exact virtual node currently hosting it.
  private var registrations: [ClusterSystem.ActorID: Registration] = [:]
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
    self.registrations = self.registrations.filter { $0.value.hostID != id }
  }

  private func findVirtualNodes() {
    guard self.listeningTask == nil else {
      self.actorSystem.log.debug("Already looking for nodes")
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
    for placement in (try? await virtualNode.actorPlacements()) ?? [] {
      guard self.registrations[placement.actorID] == nil else { continue }
      self.registrations[placement.actorID] = Registration(
        virtualID: placement.virtualActorID,
        hostID: virtualNode.id
      )
    }
  }

  /// The receptionist listing is delivered asynchronously. A lookup can
  /// therefore arrive after a node has checked in but before the listing
  /// task has added it to the ring. Refresh the ring from the receptionist's
  /// current state before reporting that no node is available.
  private func node(for id: VirtualActorID) async -> VirtualNode? {
    if let node = self.virtualNodes.getNode(for: id.rawValue) {
      return node
    }

    for node in await self.actorSystem.receptionist.lookup(VirtualNode.key) {
      await self.add(node: node)
    }

    return self.virtualNodes.getNode(for: id.rawValue)
  }

  private func node(withID id: ClusterSystem.ActorID) async -> VirtualNode? {
    if let node = self.virtualNodes.nodes.first(where: { $0.id == id }) {
      return node
    }

    for node in await self.actorSystem.receptionist.lookup(VirtualNode.key) {
      await self.add(node: node)
    }
    return self.virtualNodes.nodes.first(where: { $0.id == id })
  }

  /// - Parameters:
  /// - id—external (not system) id of an actor.
  /// - dependency—only needed when spawning an actor.
  distributed func getActor<A: VirtualActor, D: Sendable & Codable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A {
    if let (actorID, registration) = self.registrations.first(where: { $0.value.virtualID == id }) {
      guard let hostingNode = await self.node(withID: registration.hostID) else {
        throw Error.actorHostUnavailable
      }
      do {
        let actor: A = try await hostingNode.findActor(identifiedBy: id)
        guard actor.id == actorID else {
          throw VirtualNodeError.typeMismatch
        }
        return actor
      } catch VirtualNodeError.actorIsMissing {
        self.registrations[actorID] = nil
      }
    }

    guard let node = await self.node(for: id) else { throw Error.noNodesAvailable }

    let task = self.getTaskForActor(
      actorType: A.self,
      identifiedBy: id,
      on: node,
      dependency: dependency
    )

    let actor = try await task.value
    guard let typed = actor as? A else { throw VirtualNodeError.typeMismatch }
    self.registrations[actor.id] = Registration(
      virtualID: id,
      hostID: node.id
    )
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
        self.actorSystem.log.debug("Getting actor \(id) from \(node.id)")
        let actor: A = try await node.findActor(identifiedBy: id)
        return actor
      } catch {
        switch error {
        /// If there are no actors available—let's try to build it
        case VirtualNodeError.actorIsMissing:
          /// Register actor on this node (for future lookups)
          self.actorSystem.log.debug("Registered actor \(id) on \(node.id)")
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
    guard let registration = self.registrations[id] else { return }

    if let task = self.inFlightCleaning[id] {
      return try await task.value
    }
    let task = Task<Void, any Swift.Error> {
      defer {
        self.inFlightCleaning[id] = nil
      }
      self.actorSystem.log.debug("Marking \(id) as inactive, virtualId: \(registration.virtualID)")
      guard let node = await self.node(withID: registration.hostID) else {
        self.registrations[id] = nil
        return
      }
      do {
        try await node.removeActor(identifiedBy: registration.virtualID)
      } catch {
        // Keep the mapping: the node still holds the actor, so a later
        // clean (or lookup) must be able to find it again.
        self.actorSystem.log.error("Failed to remove actor \(registration.virtualID) from node \(node.id): \(error)")
        throw error
      }
      self.registrations[id] = nil
    }
    self.inFlightCleaning[id] = task
    return try await task.value
  }

  /// The consensual counterpart of ``cleanActor(identifiedBy:)``: asks the
  /// actor whether it agrees to deactivate and removes it from its node only
  /// on an affirmative answer. Returns `false` on refusal, `true` once the
  /// actor is gone (or was never known to this router).
  /// ``deactivateActor(identifiedBy:)-(ClusterSystem.ActorID)`` for the instance of the actor with `virtualID`, if it runs.
  distributed func deactivateActor(identifiedBy virtualID: VirtualActorID) async throws -> Bool {
    guard let (id, _) = self.registrations.first(where: { $0.value.virtualID == virtualID }) else { return true }
    return try await self.deactivateActor(identifiedBy: id)
  }

  distributed func deactivateActor(identifiedBy id: ClusterSystem.ActorID) async throws -> Bool {
    guard let registration = self.registrations[id] else { return true }
    guard let node = await self.node(withID: registration.hostID) else {
      self.registrations[id] = nil
      return true
    }
    let deactivated = try await node.deactivateActor(identifiedBy: registration.virtualID)
    if deactivated {
      self.registrations[id] = nil
    }
    return deactivated
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
