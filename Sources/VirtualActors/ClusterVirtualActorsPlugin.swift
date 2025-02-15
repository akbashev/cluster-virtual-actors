import Distributed
import DistributedCluster

typealias DefaultDistributedActorSystem = ClusterSystem

/// Cluster system plugin to get an actor by some id
public actor ClusterVirtualActorsPlugin {

  public enum Error: Swift.Error {
    case factoryError
    case factoryMissing
  }

  private var actorSystem: ClusterSystem!
  private var router: VirtualNodeRouter!
  private let replicationFactor: Int
  private let idleTimeoutSettings: VirtualNode.IdleTimeoutSettings

  /// Get an actor and if it's not available—create it
  public func getActor<A: VirtualActor, D: Codable & Sendable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A where A: Codable {
    guard let router else { throw Error.factoryMissing }
    return try await self.router.getActor(
      identifiedBy: id,
      dependency: dependency
    )
  }

  public init(
    replicationFactor: Int = 100,
    idleTimeoutSettings: VirtualNode.IdleTimeoutSettings = .init(
      isEnabled: false,
      cleaningInterval: .seconds(60),
      timeout: .seconds(10 * 60)
    )
  ) {
    self.replicationFactor = replicationFactor
    self.idleTimeoutSettings = idleTimeoutSettings
  }

  // TODO: Should it be fire and forget or better make it await?
  nonisolated func markAsActive<A: VirtualActor>(actor: A) {
    Task { try? await self.router.markAsActive(actor: actor) }
  }

  // TODO: Should it be fire and forget or better make it await?
  nonisolated func cleanActor(identifiedBy id: ClusterSystem.ActorID) {
    Task { try? await self.router.cleanActor(identifiedBy: id) }
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

  nonisolated public func onActorReady<Act: DistributedActor>(_ actor: Act)
  where Act.ID == ClusterSystem.ActorID {
    // no-op
  }

  nonisolated public func onResignID(_ id: ClusterSystem.ActorID) {
    self.cleanActor(identifiedBy: id)
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

extension VirtualActor {
  public func markAsActive() {
    self.actorSystem.virtualActors.markAsActive(actor: self)
  }
}
