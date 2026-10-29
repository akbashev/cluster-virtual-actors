import Distributed
@preconcurrency import DistributedCluster

typealias DefaultDistributedActorSystem = ClusterSystem

/// Cluster system plugin to get an actor by some id
public actor ClusterVirtualActorsPlugin {

  /// Running once its system starts it, with the router it reaches actors through; stopped before
  /// that, and once its system stops it.
  private var state = State.stopped

  private enum State {
    case stopped
    case running(ClusterSystem, VirtualNodeRouter)
  }

  /// Its system and router, while it runs.
  private func running() throws(VirtualActorError) -> (actorSystem: ClusterSystem, router: VirtualNodeRouter) {
    guard case .running(let actorSystem, let router) = self.state else { throw .notRunning }
    return (actorSystem, router)
  }
  private let replicationFactor: UInt64
  private let idleTimeoutSettings: VirtualNode.IdleTimeoutSettings

  /// This node's reference for each virtual actor, made once and reused, so references to the
  /// same actor on this node are equal.
  private var references: [VirtualActorID: ClusterSystem.ActorID] = [:]
  /// Where each actor's current instance runs, as last seen; asked again from the router when a
  /// call finds the instance gone.
  private var locations: [VirtualActorID: ClusterSystem.ActorID] = [:]

  /// The actor identified by `id`, made with `dependency` when there's none. It's got as a
  /// reference that stays valid while the actor deactivates, restarts or moves: each call reaches
  /// its current instance, activating it again if it isn't running. It can be kept, and sent to
  /// other nodes.
  public func getActor<A: VirtualActor, D: Codable & Sendable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A where A: Codable {
    let (actorSystem, router) = try self.running()
    let address = try VirtualActorAddress(id: id, actorType: A.self, dependency: dependency)
    // Once it's got, it's there: made now when there's none, not only when something calls it.
    self.locations[id] = try await address.instanceID(using: router)
    if let reference = self.references[id] {
      return try A.resolve(id: reference, using: actorSystem)
    }
    let metadata = ActorMetadata()
    metadata.virtualActor = address
    let actor = try actorSystem.interceptCalls(
      to: A.self,
      metadata: metadata,
      interceptor: VirtualActorInterceptor(plugin: self, address: address)
    )
    self.references[id] = actor.id
    return actor
  }

  /// The current instance itself, activated if it isn't running. For the plugin's own use and
  /// tests: callers hold references, which outlive instances.
  func instance<A: VirtualActor, D: Codable & Sendable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A where A: Codable {
    try await self.running().router.getActor(identifiedBy: id, dependency: dependency)
  }

  /// Where the current instance of the actor at `address` runs, activating it if needed.
  func location(of address: VirtualActorAddress) async throws -> (ClusterSystem, ClusterSystem.ActorID) {
    let (actorSystem, router) = try self.running()
    if let location = self.locations[address.id] {
      return (actorSystem, location)
    }
    let location = try await address.instanceID(using: router)
    self.locations[address.id] = location
    return (actorSystem, location)
  }

  /// Forgets that the actor at `id` runs at `location`, after a call found it gone.
  func forget(_ id: VirtualActorID, at location: ClusterSystem.ActorID) {
    if self.locations[id] == location {
      self.locations[id] = nil
    }
  }

  /// Evict an actor from the registry unconditionally, without asking
  /// `shouldDeactivate()`. The instance is not stopped — in-flight work
  /// continues and ARC frees it once nothing references it; the next lookup
  /// by id spawns a fresh instance. Internal on purpose: evicting a live
  /// actor from the outside duplicates it (the registry forgets while the
  /// instance keeps running). Used by the library's own bookkeeping — the
  /// resign hook fires only after the instance is actually gone — and by
  /// actor self-eviction (``VirtualActor/resign()``). External callers
  /// should use ``deactivate(_:)``, which asks first.
  func cleanActor(_ actor: any VirtualActor) async throws {
    try await self.cleanActor(identifiedBy: actor.id)
  }

  /// The consensual counterpart of ``cleanActor(_:)``: asks the actor — on
  /// its own executor, via `shouldDeactivate()` — and evicts it only on an
  /// affirmative answer. The immediate, final version of the idle sweep:
  /// eviction is full removal, not a downgrade, and any in-memory state is
  /// lost. Returns `false` if the actor refused.
  ///
  /// - Parameter actor: A reference from ``getActor(identifiedBy:dependency:)``, or the actor
  ///   itself, e.g. `self` inside it.
  @discardableResult
  public func deactivate(_ actor: any VirtualActor) async throws -> Bool {
    if let address = actor.id.metadata.virtualActor {
      self.locations[address.id] = nil
      return try await self.running().router.deactivateActor(identifiedBy: address.id)
    }
    self.locations = self.locations.filter { $0.value != actor.id }
    return try await self.running().router.deactivateActor(identifiedBy: actor.id)
  }

  /// A plugin to install in `settings`:
  /// ```swift
  /// let plugin = ClusterVirtualActorsPlugin.configuring(&settings)
  /// settings.plugins.install(plugin: plugin)
  /// ```
  ///
  /// - Parameter settings: The settings the plugin is installed in. It declares there that references
  ///   to virtual actors carry their address to other nodes, so they work wherever they're sent.
  public static func configuring(
    _ settings: inout ClusterSystemSettings,
    replicationFactor: UInt64 = 100,
    idleTimeoutSettings: VirtualNode.IdleTimeoutSettings = .default
  ) -> ClusterVirtualActorsPlugin {
    settings.propagateMetadata(\.virtualActor)
    return ClusterVirtualActorsPlugin(
      replicationFactor: replicationFactor,
      idleTimeoutSettings: idleTimeoutSettings
    )
  }

  /// For the package's own tests, which don't send references between nodes.
  init(
    replicationFactor: UInt64 = 100,
    idleTimeoutSettings: VirtualNode.IdleTimeoutSettings = .default
  ) {
    self.replicationFactor = replicationFactor
    self.idleTimeoutSettings = idleTimeoutSettings
  }

  private func cleanActor(identifiedBy id: ClusterSystem.ActorID) async throws {
    try await self.running().router.cleanActor(identifiedBy: id)
  }

}

extension ClusterVirtualActorsPlugin: ActorLifecyclePlugin {

  static let pluginKey: Key = "$clusterVirtualActors"

  public nonisolated var key: Key {
    Self.pluginKey
  }

  public func start(_ system: ClusterSystem) async throws {
    let router = try await system.singleton.host(name: "virtual_actor_node_router") {
      [replicationFactor, idleTimeoutSettings] actorSystem in
      await VirtualNodeRouter(
        actorSystem: actorSystem,
        replicationFactor: replicationFactor,
        idleTimeoutSettings: idleTimeoutSettings
      )
    }
    self.state = .running(system, router)
  }

  public func stop(_ system: ClusterSystem) async {
    self.state = .stopped
  }

  nonisolated public func onActorReady<Act: DistributedActor>(_ actor: Act) where Act.ID == ClusterSystem.ActorID {
    // no-op
  }

  /// Intercepts references to virtual actors received from other nodes.
  nonisolated public func interceptor(for id: ClusterSystem.ActorID) -> (any RemoteCallInterceptor)? {
    id.metadata.virtualActor.map { VirtualActorInterceptor(plugin: self, address: $0) }
  }

  nonisolated public func onResignID(_ id: ClusterSystem.ActorID) {
    Task { [weak self] in
      await self?.cleanAfterResign(id)
    }
  }

  /// Runs isolated on the plugin actor so `stop()` cannot nil `router`
  /// between the check and the use (shutdown race).
  private func cleanAfterResign(_ id: ClusterSystem.ActorID) async {
    self.locations = self.locations.filter { $0.value != id }
    guard case .running(_, let router) = self.state else { return }
    try? await router.cleanActor(identifiedBy: id)
  }
}

extension ClusterSystem {

  public var virtualActors: ClusterVirtualActorsPlugin {
    let key = ClusterVirtualActorsPlugin.pluginKey
    guard let actorPlugin = self.settings.plugins[key] else {
      fatalError("No plugin found for key: [\(key)], installed plugins: \(self.settings.plugins)")
    }
    return actorPlugin
  }
}
