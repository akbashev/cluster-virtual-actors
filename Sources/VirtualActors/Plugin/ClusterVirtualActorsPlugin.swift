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

  public func cleanActor(_ actor: any VirtualActor) async throws {
    try await self.cleanActor(identifiedBy: actor.id)
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

  nonisolated public func onResignID(_ id: ClusterSystem.ActorID) {
    Task { [weak self] in
      guard await self?.router != nil else { return }
      try await self?.cleanActor(identifiedBy: id)
    }
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
