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
        await self.checkTimedOutActors(idleTimeoutSettings: idleTimeoutSettings)
      }
    }
  }

  private func checkTimedOutActors(idleTimeoutSettings: IdleTimeoutSettings) async {
    var idsToAsk: [VirtualActorID] = []
    var staleEntries: [VirtualActorID] = []
    for (id, reference) in self.storage.virtualActors {
      if reference.actor == nil {
        // The weak hold expired — the actor was freed. Prune the stale entry.
        staleEntries.append(id)
      } else if reference.isWarm,
        ContinuousClock.now - reference.lastUpdated >= idleTimeoutSettings.timeout
      {
        idsToAsk.append(id)
      }
    }
    for id in staleEntries {
      self.removeActor(identifiedBy: id)
    }
    guard !idsToAsk.isEmpty else { return }
    self.actorSystem.log.debug("Found inactive actor \(idsToAsk), asking to deactivate...")
    // Serial on purpose: the cleaning task is the only asker and it awaits
    // each answer, so an entry can never have two questions in flight.
    for id in idsToAsk {
      await self.askToDeactivate(identifiedBy: id)
    }
  }

  /// The idle sweep: ask the actor whether it is ready to be deactivated. The
  /// question executes *on the actor* (`whenLocal` hops onto its executor),
  /// serialized with its synchronous execution — it may still interleave with
  /// a *suspending* async turn, so refusal rests on the actor's domain state
  /// (see ``VirtualActor/shouldDeactivate()``). An affirmative answer
  /// downgrades the registry's hold (the actor's own cleanup already ran
  /// inside the question); a negative one bumps the idle clock.
  private func askToDeactivate(identifiedBy id: VirtualActorID) async {
    guard let reference = self.storage.virtualActors[id], let actor = reference.actor else { return }
    let askedAt = reference.lastUpdated
    let answer = await actor.whenLocal { await $0.shouldDeactivate() } ?? true
    if !answer {
      reference.lastUpdated = .now
    } else if reference.lastUpdated == askedAt {
      // Downgrade only if nothing touched the entry while the question was
      // being answered — a lookup in between already counts as activity.
      self.storage.downgradeActor(identifiedBy: id)
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
  /// A storage entry for one actor. Entries start *warm*: the registry
  /// holds the actor strongly, keeping it alive between resolutions. The
  /// idle sweep first asks the actor whether it should deactivate
  /// (``VirtualActor/shouldDeactivate()``); only on an affirmative answer
  /// does the sweep downgrade the entry to a weak hold — from then on the
  /// actor survives only on references held elsewhere (ARC frees it when
  /// there are none), and a later lookup *revives* the entry instead of
  /// spawning a duplicate instance of the same `VirtualActorID`.
  fileprivate final class Reference {
    private var strongActor: (any VirtualActor)?
    private weak var weakActor: (any VirtualActor)?
    var lastUpdated: ContinuousClock.Instant

    /// The actor while it lives anywhere — registry hold or not.
    var actor: (any VirtualActor)? { self.strongActor ?? self.weakActor }
    /// Whether the registry itself still keeps the actor alive.
    var isWarm: Bool { self.strongActor != nil }

    init(actor: any VirtualActor, lastUpdated: ContinuousClock.Instant = .now) {
      self.strongActor = actor
      self.lastUpdated = lastUpdated
    }

    /// The idle sweep, after the actor agreed to deactivate: drop the
    /// registry's own hold. The actor stays reachable through the weak hold
    /// (for revival) until ARC frees it.
    func downgrade() {
      self.weakActor = self.strongActor
      self.strongActor = nil
    }

    /// A lookup on a swept entry: the registry warms the actor again.
    func revive() {
      if self.strongActor == nil {
        self.strongActor = self.weakActor
      }
    }
  }

  private(set) var virtualActors: [VirtualActorID: Reference] = [:]

  func getActor(identifiedBy id: VirtualActorID) -> (any VirtualActor)? {
    guard let reference = self.virtualActors[id] else { return nil }
    // A swept entry whose weak hold expired is treated as missing: the
    // caller respawns a fresh instance (and overwrites the stale entry).
    guard let actor = reference.actor else { return nil }
    defer { reference.lastUpdated = .now }
    reference.revive()
    return actor
  }

  mutating func insertActor(_ actor: any VirtualActor, identifiedBy id: VirtualActorID) {
    self.virtualActors[id] = .init(actor: actor)
  }

  /// The sweep operation, performed after the actor agreed to deactivate:
  /// the registry drops its own hold on the actor (see
  /// ``Reference/downgrade``). The actor is NOT removed from storage —
  /// a weak hold remains so a later lookup revives the same instance
  /// instead of duplicating it.
  mutating func downgradeActor(identifiedBy id: VirtualActorID) {
    self.virtualActors[id]?.downgrade()
  }

  mutating func removeActor(identifiedBy id: VirtualActorID) {
    self.virtualActors.removeValue(forKey: id)
  }
}
