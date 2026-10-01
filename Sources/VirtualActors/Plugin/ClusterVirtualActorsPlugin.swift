import Distributed
@preconcurrency import DistributedCluster

typealias DefaultDistributedActorSystem = ClusterSystem

/// Cluster system plugin to get an actor by some id
public actor ClusterVirtualActorsPlugin {

  private var actorSystem: ClusterSystem!
  private var router: VirtualNodeRouter!
  private let replicationFactor: UInt64
  private let idleTimeoutSettings: VirtualNode.IdleTimeoutSettings

  /// Get an actor and if it's not available—create it
  public func getActor<A: VirtualActor, D: Codable & Sendable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A where A: Codable {
    try await self.router.getActor(
      identifiedBy: id,
      dependency: dependency
    )
  }

  /// A reference to the actor that stays valid while the actor deactivates, restarts or moves:
  /// each call reaches its current instance, activating it with `dependency` if it isn't running.
  /// Unlike ``getActor(identifiedBy:dependency:)``, which returns one instance, the reference can be
  /// kept, and sent to other nodes that installed the plugin with
  /// `ClusterSystemSettings.installVirtualActors(_:)`.
  public func reference<A: VirtualActor, D: Codable & Sendable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) throws -> A {
    let address = try VirtualActorAddress(id: id, actorType: A.self, dependency: dependency)
    let metadata = ActorMetadata()
    metadata.virtualActor = address
    return try self.actorSystem.interceptCalls(
      to: A.self,
      metadata: metadata,
      interceptor: VirtualActorInterceptor(plugin: self, address: address)
    )
  }

  /// The current instance of the actor at `address`, activated if it isn't running.
  func instance(of address: VirtualActorAddress) async throws -> (ClusterSystem, ClusterSystem.ActorID) {
    (self.actorSystem, try await address.instanceID(using: self.router))
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
  @discardableResult
  public func deactivate(_ actor: any VirtualActor) async throws -> Bool {
    try await self.router.deactivateActor(identifiedBy: actor.id)
  }

  public init(
    replicationFactor: UInt64 = 100,
    idleTimeoutSettings: VirtualNode.IdleTimeoutSettings = .default
  ) {
    self.replicationFactor = replicationFactor
    self.idleTimeoutSettings = idleTimeoutSettings
  }

  private func cleanActor(identifiedBy id: ClusterSystem.ActorID) async throws {
    try await self.router.cleanActor(identifiedBy: id)
  }

}

extension ClusterVirtualActorsPlugin: ActorLifecyclePlugin {

  static let pluginKey: Key = "$clusterVirtualActors"

  public nonisolated var key: Key {
    Self.pluginKey
  }

  public func start(_ system: ClusterSystem) async throws {
    self.actorSystem = system
    self.router = try await system.singleton.host(name: "virtual_actor_node_router") {
      [replicationFactor, idleTimeoutSettings] actorSystem in
      await VirtualNodeRouter(
        actorSystem: actorSystem,
        replicationFactor: replicationFactor,
        idleTimeoutSettings: idleTimeoutSettings
      )
    }
  }

  public func stop(_ system: ClusterSystem) async {
    self.actorSystem = nil
    self.router = nil
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
    guard let router = self.router else { return }
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
