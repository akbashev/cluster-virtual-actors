import Distributed
import DistributedCluster
import Testing

@testable import VirtualActors

protocol StoreKind: Sendable {
  associatedtype Value: Codable & Sendable
}

struct GenericVirtualActorTests {
  enum Numbers: StoreKind {
    typealias Value = Int
  }

  distributed actor Store<Kind: StoreKind>: VirtualActor {
    typealias ActorSystem = ClusterSystem

    struct None: Codable, Sendable {}

    init(actorSystem: ClusterSystem) {
      self.actorSystem = actorSystem
    }

    static func spawn(on actorSystem: ClusterSystem, dependency: any Sendable & Codable) async throws -> Store {
      Store(actorSystem: actorSystem)
    }

    distributed func echo(_ value: Kind.Value) -> Kind.Value {
      value
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func aGenericActorTakesItsAssociatedTypeThroughAReference() async throws {
    let system = await ClusterSystem("generic-virtual-actor") {
      $0.bindPort = 2740
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      let plugin = ClusterVirtualActorsPlugin.configuring(&$0, replicationFactor: 10)
      $0.plugins.install(plugin: plugin)
    }
    defer { try? system.shutdown() }
    let virtualNode = await VirtualNode(actorSystem: system)
    system.cluster.join(endpoint: system.cluster.endpoint)
    try await system.cluster.joined(within: .seconds(10))

    let store: Store<Numbers> = try await system.virtualActors.getActor(
      identifiedBy: VirtualActorID(rawValue: "numbers"),
      dependency: Store<Numbers>.None()
    )

    #expect(try await store.echo(42) == 42)
    _ = virtualNode
  }
}
