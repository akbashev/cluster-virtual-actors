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

  distributed actor RecoveryActor: VirtualActor {
    typealias ActorSystem = ClusterSystem

    struct None: Codable, Sendable {}

    init(actorSystem: ClusterSystem) async throws {
      self.actorSystem = actorSystem
    }

    static func spawn(
      on actorSystem: ClusterSystem,
      dependency: any Sendable & Codable
    ) async throws -> RecoveryActor {
      try await RecoveryActor(actorSystem: actorSystem)
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
    _ = node
  }

  @Test
  func testVirtualNodeReportsLiveActorsForRecovery() async throws {
    let system = await ClusterSystem("placement-report")
    let node = await VirtualNode(actorSystem: system)
    let router = await VirtualNodeRouter(
      actorSystem: system,
      replicationFactor: 10,
      idleTimeoutSettings: .default
    )
    let id = VirtualActorID(rawValue: "placement-report-actor")
    let actor: SingleFlightActor = try await router.getActor(
      identifiedBy: id,
      dependency: SingleFlightActor.None()
    )

    let placements = try await node.actorPlacements()
    #expect(placements.contains { $0.virtualActorID == id && $0.actorID == actor.id })
  }

  @Test
  func testSingletonFailoverRecoversExistingActor() async throws {
    let host = await ClusterSystem("failover-host") {
      $0.bindPort = 2690
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 3)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    }
    let survivor = await ClusterSystem("failover-survivor") {
      $0.bindPort = 2691
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 3)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    }
    let secondSurvivor = await ClusterSystem("failover-second-survivor") {
      $0.bindPort = 2692
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 3)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    }
    let replacement = await ClusterSystem("failover-replacement") {
      $0.bindPort = 2693
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 3)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    }
    let hostNode = await VirtualNode(actorSystem: host)
    let survivorNode = await VirtualNode(actorSystem: survivor)
    let secondSurvivorNode = await VirtualNode(actorSystem: secondSurvivor)
    let replacementNode = await VirtualNode(actorSystem: replacement)

    host.cluster.join(endpoint: host.cluster.endpoint)
    survivor.cluster.join(endpoint: host.cluster.endpoint)
    secondSurvivor.cluster.join(endpoint: host.cluster.endpoint)
    try await host.cluster.waitFor(
      [survivor.cluster.node, secondSurvivor.cluster.node],
      .up,
      within: .seconds(10)
    )

    let router = try await host.singleton.proxy(
      VirtualNodeRouter.self,
      name: "virtual_actor_node_router"
    )
    #expect(router.id.node == host.cluster.node)

    var actor: RecoveryActor?
    var actorID: VirtualActorID?
    for index in 0..<40 {
      let candidateID = VirtualActorID(rawValue: "failover-\(index)")
      let candidate: RecoveryActor = try await survivor.virtualActors.getActor(
        identifiedBy: candidateID,
        dependency: RecoveryActor.None()
      )
      let survivorHasActor = (try await survivorNode.actorPlacements()).contains { $0.actorID == candidate.id }
      let secondSurvivorHasActor = (try await secondSurvivorNode.actorPlacements()).contains { $0.actorID == candidate.id }
      if survivorHasActor || secondSurvivorHasActor {
        actor = candidate
        actorID = candidateID
        break
      }
    }

    guard let actor, let actorID else {
      Issue.record("Could not place a test actor on the surviving node")
      return
    }

    host.cluster.leave()
    try await survivor.cluster.waitFor(host.cluster.node, .down, within: .seconds(10))
    try await secondSurvivor.cluster.waitFor(host.cluster.node, .down, within: .seconds(10))

    replacement.cluster.join(endpoint: survivor.cluster.endpoint)
    try await survivor.cluster.waitFor(
      [secondSurvivor.cluster.node, replacement.cluster.node],
      .up,
      within: .seconds(10)
    )

    var recovered: RecoveryActor?
    for _ in 0..<100 {
      do {
        recovered = try await survivor.virtualActors.getActor(
          identifiedBy: actorID,
          dependency: RecoveryActor.None()
        )
        break
      } catch VirtualNodeRouter.Error.noNodesAvailable {
        try await Task.sleep(for: .milliseconds(100))
      } catch {
        throw error
      }
    }

    #expect(recovered?.id == actor.id)
    _ = hostNode
    _ = secondSurvivorNode
    _ = replacementNode
  }
}
