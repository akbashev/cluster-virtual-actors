import AsyncAlgorithms
import Distributed
import DistributedCluster

distributed public actor VirtualNode: Routable {

  nonisolated var address: Cluster.Node { self.actorSystem.cluster.node }

  private var storage: ActorStorage = ActorStorage()

  distributed public func findActor<A: VirtualActor>(identifiedBy id: VirtualActorID) throws -> A {
    guard let actor = self.storage.getActor(identifiedBy: id) else { throw VirtualNodeError.actorIsMissing }
    guard let actor = actor as? A else { throw VirtualNodeError.typeMismatch }
    return actor
  }

  distributed func spawnActor<A: VirtualActor, D: Sendable & Codable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A {
    let actor = try await A.spawn(on: self.actorSystem, dependency: dependency)
    self.storage.insertActor(actor, identifiedBy: id)
    return actor
  }

  distributed func updateTimeoutSettings(_ settings: IdleTimeoutSettings) {
    if settings.isEnabled {
      self.startCleaning(using: settings)
    } else {
      self.cleaningTask?.cancel()
      self.cleaningTask = nil
    }
  }

  distributed func removeActor(identifiedBy id: VirtualActorID) {
    self.storage.removeActor(identifiedBy: id)
  }

  public distributed func run() async throws {
    try await self.actorSystem.terminated
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
    guard idleTimeoutSettings.isEnabled, self.cleaningTask == nil else { return }
    let sequence = AsyncTimerSequence<ContinuousClock>(
      interval: idleTimeoutSettings.cleaningInterval,
      clock: .continuous
    )
    self.cleaningTask = Task {
      for await _ in sequence {
        guard !Task.isCancelled else { return }
        self.checkTimedOutActors(idleTimeoutSettings: idleTimeoutSettings)
      }
    }
  }

  private func checkTimedOutActors(idleTimeoutSettings: IdleTimeoutSettings) {
    var idsToRemove: [VirtualActorID] = []
    for (id, reference) in self.storage.virtualActors where (ContinuousClock.now - reference.lastUpdated >= idleTimeoutSettings.timeout) {
      idsToRemove.append(id)
    }
    guard !idsToRemove.isEmpty else { return }
    self.actorSystem.log.info("Found inactive actor \(idsToRemove), cleaning...")
    for id in idsToRemove {
      self.removeActor(identifiedBy: id)
    }
  }

  deinit {
    self.cleaningTask?.cancel()
    self.cleaningTask = nil
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
  public struct IdleTimeoutSettings: Sendable, Codable {

    public static let `default` = IdleTimeoutSettings(
      isEnabled: false,
      cleaningInterval: .seconds(60),
      timeout: .seconds(10 * 60)
    )

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

private struct ActorStorage {
  fileprivate class Reference {
    let actor: any VirtualActor
    var lastUpdated: ContinuousClock.Instant

    init(actor: any VirtualActor, lastUpdated: ContinuousClock.Instant = .now) {
      self.actor = actor
      self.lastUpdated = lastUpdated
    }
  }

  private(set) var virtualActors: [VirtualActorID: Reference] = [:]

  func getActor(identifiedBy id: VirtualActorID) -> (any VirtualActor)? {
    guard let reference = self.virtualActors[id] else { return nil }
    defer { reference.lastUpdated = .now }
    return reference.actor
  }

  mutating func insertActor(_ actor: any VirtualActor, identifiedBy id: VirtualActorID) {
    self.virtualActors[id] = .init(actor: actor)
  }

  mutating func removeActor(identifiedBy id: VirtualActorID) {
    self.virtualActors.removeValue(forKey: id)
  }
}
