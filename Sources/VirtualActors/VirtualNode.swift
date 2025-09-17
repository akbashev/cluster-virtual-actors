import AsyncAlgorithms
import Distributed
import DistributedCluster

distributed public actor VirtualNode: Routable {

  nonisolated var address: Cluster.Node { self.actorSystem.cluster.node }

  private var virtualActors: [VirtualActorID: Reference] = [:]

  distributed public func findActor<A: VirtualActor>(identifiedBy id: VirtualActorID) throws -> A {
    guard let reference = self.virtualActors[id] else { throw VirtualNodeError.actorIsMissing }
    guard let actor = reference.actor as? A else { throw VirtualNodeError.typeMismatch }
    self.virtualActors[id]?.lastUpdated = .now
    return actor
  }

  // TODO: Check for reentrancy issues and probably add queueing
  distributed func spawnActor<A: VirtualActor, D: Sendable & Codable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A {
    let actor = try await A.spawn(on: self.actorSystem, dependency: dependency)
    actor.metadata[keyPath: \.virtualId] = id
    self.virtualActors[id] = .init(actor: actor)
    return actor
  }

  distributed func markActorAsActive(identifiedBy id: VirtualActorID) {
    self.virtualActors[id]?.lastUpdated = .now
    self.actorSystem.log.debug("Marking actor \(id) as active")
  }

  distributed func updateTimeoutSettings(_ settings: IdleTimeoutSettings) {
    if settings.isEnabled {
      self.startCleaning(using: settings)
    } else {
      self.cleaningTask?.cancel()
    }
  }

  public init(
    actorSystem: ClusterSystem
  ) async {
    self.actorSystem = actorSystem
    await actorSystem
      .receptionist
      .checkIn(self, with: Self.key)
  }

  private var cleaningTask: Task<Void, Never>?
  private func startCleaning(using idleTimeoutSettings: IdleTimeoutSettings) {
    guard idleTimeoutSettings.isEnabled else { return }
    let sequence = AsyncTimerSequence<ContinuousClock>(
      interval: idleTimeoutSettings.cleaningInterval,
      clock: .continuous
    )
    self.cleaningTask = Task {
      for await _ in sequence {
        guard !Task.isCancelled else { return }
        for (id, reference) in self.virtualActors
        where (ContinuousClock.now - reference.lastUpdated >= idleTimeoutSettings.timeout) {
          self.actorSystem.log.info("Found inactive actor \(id), cleaning...")
          self.virtualActors.removeValue(forKey: id)
        }
      }
    }
  }

  public distributed func run() async throws {
    try await self.actorSystem.terminated
  }

  deinit {
    self.cleaningTask?.cancel()
  }
}

public enum VirtualNodeError: Error, Codable {
  case actorIsMissing
  case typeMismatch
}

extension VirtualNode {
  static var key: DistributedReception.Key<VirtualNode> { "virtual_node_distributed_key" }
}

extension VirtualNode {
  fileprivate class Reference {
    let actor: any VirtualActor
    var lastUpdated: ContinuousClock.Instant

    init(actor: any VirtualActor, lastUpdated: ContinuousClock.Instant = .now) {
      self.actor = actor
      self.lastUpdated = lastUpdated
    }
  }
}

extension VirtualNode {
  public struct IdleTimeoutSettings: Sendable, Codable {
    let isEnabled: Bool
    let cleaningInterval: ContinuousClock.Duration
    let timeout: ContinuousClock.Duration

    public init(
      isEnabled: Bool,
      cleaningInterval: ContinuousClock.Duration,
      timeout: ContinuousClock.Duration
    ) {
      self.isEnabled = isEnabled
      self.cleaningInterval = cleaningInterval
      self.timeout = timeout
    }
  }
}

extension ActorMetadataKeys {
  var virtualId: Key<VirtualActorID> { "$virtual-actor-id" }
}
