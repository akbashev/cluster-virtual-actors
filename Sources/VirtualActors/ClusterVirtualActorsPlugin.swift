import Distributed
import DistributedCluster

/// Cluster system plugin to get an actor by some id
public actor ClusterVirtualActorsPlugin {
    
  public enum Error: Swift.Error {
    case factoryError
    case factoryMissing
  }
  
  private var actorSystem: ClusterSystem!
  private var router: VirtualNodeRouter!
    
  /// Get an actor and if it's not available—create it
  public func getActor<A: VirtualActor>(
    withId id: VirtualActorID,
    _ build: @Sendable (ClusterSystem) async throws -> A
  ) async throws -> A {
    do {
      /// Try to get an actor by id
      return try await self.getActor(withId: id)
    } catch {
      switch error {
      /// If there are no actors available—let's try to build it
      case VirtualNodeRouter.Error.noActorsAvailable:
        /// Get appropriate node
        let node = try await router.getNode(for: id)
        /// Pass cluster system of this node
        let actor = try await build(node.actorSystem)
        /// Register actor on this node (for future lookups)
        try await node.register(
          actor: actor,
          with: actor.virtualID
        )
        return actor
      default:
        throw error
      }
    }
  }
  
  /// Just get an actor
  public func getActor<A: VirtualActor>(
    withId id: VirtualActorID
  ) async throws -> A {
    guard let router else { throw Error.factoryMissing }
    return try await router.get(id: id)
  }

  public init() {}
}

extension ClusterVirtualActorsPlugin: ActorLifecyclePlugin {
  
  static let pluginKey: Key = "$clusterVirtualActors"
  
  public nonisolated var key: Key {
    Self.pluginKey
  }
  
  public func start(_ system: ClusterSystem) async throws {
    self.actorSystem = system
    self.router = try await system.singleton.host(name: "virtual_actor_node_router") { actorSystem in
      await VirtualNodeRouter(
        actorSystem: actorSystem
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
    Task { [weak self] in try await self?.router?.close(with: id) }
  }
  
}

extension ClusterSystem {
  
  public var virtualActors: ClusterVirtualActorsPlugin {
    let key = ClusterVirtualActorsPlugin.pluginKey
    guard let journalPlugin = self.settings.plugins[key] else {
      fatalError("No plugin found for key: [\(key)], installed plugins: \(self.settings.plugins)")
    }
    return journalPlugin
  }
}
