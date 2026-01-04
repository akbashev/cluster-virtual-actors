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
    let system = await ClusterSystem("single-flight")
    let node = await VirtualNode(actorSystem: system)
    try await Task.sleep(for: .seconds(1))
    let router = await VirtualNodeRouter(
      actorSystem: system,
      replicationFactor: 10,
      idleTimeoutSettings: .init(
        isEnabled: false,
        cleaningInterval: .seconds(60),
        timeout: .seconds(600)
      )
    )

    let id = VirtualActorID(rawValue: "single-flight-actor")

    let (ids, responses) = try await withThrowingTaskGroup(
      of: ClusterSystem.ActorID.self,
      returning: (Set<ClusterSystem.ActorID>, Int).self
    ) { group in
      for _ in 0..<20 {
        group.addTask {
          let actor: SingleFlightActor = try await router.getActor(
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
