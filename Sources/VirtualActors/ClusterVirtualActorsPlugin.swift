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

  /// Get an actor and if it's not available—create it
  public func getActor<A: VirtualActor, D: Codable & Sendable>(
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A where A: Codable {
    guard let router else { throw Error.factoryMissing }
    let node = try await self.router.getNode(identifiedBy: id)
    do {
      /// Try to get an actor by id
      self.actorSystem.log.info("Getting actor \(id) from \(node.id)")
      return try await node.findActor(identifiedBy: id)
    } catch {
      switch error {
        /// If there are no actors available—let's try to build it
      case VirtualNodeError.actorIsMissing:
        /// Register actor on this node (for future lookups)
        self.actorSystem.log.info("Registered actor \(id) on \(node.id)")
        return try await node.spawn(
          identifiedBy: id,
          dependency: dependency
        )
      default:
        throw error
      }
    }
  }

  public init(
    replicationFactor: Int = 100
  ) {
    self.replicationFactor = replicationFactor
  }
}

extension ClusterVirtualActorsPlugin: ActorLifecyclePlugin {
  
  static let pluginKey: Key = "$clusterVirtualActors"
  
  public nonisolated var key: Key {
    Self.pluginKey
  }
  
  public func start(_ system: ClusterSystem) async throws {
    self.actorSystem = system
    let replicationFactor = self.replicationFactor
    self.router = try await system.singleton.host(name: "virtual_actor_node_router") { actorSystem in
      await VirtualNodeRouter(
        actorSystem: actorSystem,
        replicationFactor: replicationFactor
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
    // no-op
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
