import Distributed
import DistributedCluster
import Testing

@testable import VirtualActors

struct SingleFlightTests {
  distributed actor SingleFlightActor: VirtualActor {
    typealias ActorSystem = ClusterSystem

    struct None: Codable, Sendable {}

    init(actorSystem: ClusterSystem) async throws {
      self.actorSystem = actorSystem
      // Random timeout
      let delays = Int.random(in: 500...1000)
      try await Task.sleep(for: .milliseconds(delays))
    }

    static func spawn(
      on actorSystem: ClusterSystem,
      dependency: any Sendable & Codable
    ) async throws -> SingleFlightActor {
      try await SingleFlightActor(
        actorSystem: actorSystem
      )
    }
  }

  @Test
  func testGetActorSingleFlight() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "single-flight") {
      $0.bindPort = 2650
      // For singleton plugin to work we need to choose a leader by having 1 member
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(
        plugin: ClusterVirtualActorsPlugin(
          replicationFactor: 10
        )
      )
    }

    system.cluster.join(endpoint: system.cluster.endpoint)
    try await system.cluster.joined(within: .seconds(3))

    let id = VirtualActorID(rawValue: "single-flight-actor")

    let (ids, responses) = try await withThrowingTaskGroup(
      of: ClusterSystem.ActorID.self,
      returning: (Set<ClusterSystem.ActorID>, Int).self
    ) { group in
      for _ in 0..<20 {
        group.addTask {
          let actor: SingleFlightActor = try await system.virtualActors.getActor(
            identifiedBy: id,
            dependency: SingleFlightActor.None()
          )
          return actor.id
        }
      }
      return try await group.reduce(
        into: ([], 0),
        { acc, next in
          acc.0.insert(next)
          acc.1 += 1
        }
      )
    }

    #expect(responses == 20, "All concurrent calls should return.")
    #expect(ids.count == 1, "Concurrent getActor calls should return the same actor.")
  }
}
