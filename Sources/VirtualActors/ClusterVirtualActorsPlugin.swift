import Distributed
import DistributedCluster

public actor ClusterVirtualActorsPlugin {
    
  public enum Error: Swift.Error {
    case factoryError
    case factoryMissing
  }
  
  private var actorSystem: ClusterSystem!
  private var router: VirtualNodeRouter!
    
  public func getActor<A: VirtualActor>(
    withId id: VirtualActorID,
    _ build: @Sendable () async throws -> A
  ) async throws -> A {
    guard let router else { throw Error.factoryMissing }
    do {
      return try await router.get(id: id)
    } catch {
      switch error {
      case VirtualNodeRouter.Error.noActorsAvailable:
        let actor = try await build()
        try await router.add(actor)
        return actor
      default:
        throw error
      }
    }
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
