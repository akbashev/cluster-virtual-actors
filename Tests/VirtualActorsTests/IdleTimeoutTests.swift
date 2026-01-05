import Distributed
import DistributedCluster
import Testing

@testable import VirtualActors

@Suite(.timeLimit(.minutes(1)))
struct IdleTimeoutTests {

  distributed actor TestActor: VirtualActor {
    typealias ActorSystem = ClusterSystem

    struct None: Codable, Sendable {}

    init(actorSystem: ClusterSystem) {
      self.actorSystem = actorSystem
    }

    static func spawn(
      on actorSystem: ClusterSystem,
      dependency: any Sendable & Codable
    ) async throws -> TestActor {
      TestActor(actorSystem: actorSystem)
    }
  }

  @Test
  func testIdleTimeoutRemovesActors() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-remove") {
      $0.bindPort = 2550
      // For singleton plugin to work we need to choose a leader by having 1 member
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(
        plugin: ClusterVirtualActorsPlugin(
          replicationFactor: 10,
          idleTimeoutSettings: .init(
            isEnabled: true,
            cleaningInterval: .milliseconds(200),
            timeout: .milliseconds(500)
          )
        )
      )
    }

    system.cluster.join(endpoint: system.cluster.endpoint)
    try await system.cluster.joined(within: .seconds(3))

    let id = VirtualActorID(rawValue: "idle-timeout-remove")
    let _: TestActor = try await system.virtualActors.getActor(
      identifiedBy: id,
      dependency: TestActor.None()
    )

    try await Task.sleep(for: .milliseconds(900))

    await #expect(throws: VirtualNodeError.actorIsMissing) {
      let _: TestActor = try await node.findActor(identifiedBy: id)
    }
  }

  @Test
  func testResignRemovesLocalActor() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-resign") {
      $0.bindPort = 2551
      // For singleton plugin to work we need to choose a leader by having 1 member
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(
        plugin: ClusterVirtualActorsPlugin(
          replicationFactor: 10,
          idleTimeoutSettings: .init(
            isEnabled: false,
            cleaningInterval: .seconds(60),
            timeout: .seconds(600)
          )
        )
      )
    }

    system.cluster.join(endpoint: system.cluster.endpoint)
    try await system.cluster.joined(within: .seconds(3))

    let id = VirtualActorID(rawValue: "idle-timeout-resign")
    let actor: TestActor = try await system.virtualActors.getActor(
      identifiedBy: id,
      dependency: TestActor.None()
    )

    try await Task.sleep(for: .seconds(1))

    try await actor.resign()
    await #expect(throws: VirtualNodeError.actorIsMissing) {
      let _: TestActor = try await node.findActor(identifiedBy: id)
    }
  }

  @Test
  func testResignRemovesRemoteActor() async throws {
    let (actors, node) = await ClusterSystem.startVirtualNode(named: "actor-node") {
      $0.endpoint = Cluster.Endpoint(host: "127.0.0.1", port: 2552)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(
        plugin: ClusterVirtualActorsPlugin()
      )
    }
    let routerSystem = await ClusterSystem("router-node") {
      $0.endpoint = Cluster.Endpoint(host: "127.0.0.1", port: 2553)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(
        plugin: ClusterVirtualActorsPlugin()
      )
    }

    actors.cluster.join(endpoint: routerSystem.cluster.endpoint)
    try await routerSystem.cluster.joined(within: .seconds(3))

    let id = VirtualActorID(rawValue: "actor-a")
    let actor: TestActor = try await routerSystem.virtualActors.getActor(
      identifiedBy: id,
      dependency: TestActor.None()
    )
    try await actor.resign()
    await #expect(throws: VirtualNodeError.actorIsMissing) {
      let _: TestActor = try await node.findActor(identifiedBy: id)
    }
  }
}
