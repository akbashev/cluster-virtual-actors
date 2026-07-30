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

    /// Proves an instance is alive and answering.
    distributed func ping() -> Bool { true }
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
  func testSweptActorIsFreedWhenUnreferenced() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-free") {
      $0.bindPort = 2554
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

    let id = VirtualActorID(rawValue: "idle-timeout-free")
    weak var weakActor: TestActor?
    do {
      let actor: TestActor = try await getActorEventually(
        on: system,
        identifiedBy: id,
        dependency: TestActor.None()
      )
      weakActor = actor
    }
    // The only strong reference is gone; once the sweep downgrades the
    // registry's hold, ARC must free the instance.
    try await Task.sleep(for: .milliseconds(1500))

    #expect(weakActor == nil)
    await #expect(throws: VirtualNodeError.actorIsMissing) {
      let _: TestActor = try await node.findActor(identifiedBy: id)
    }
  }

  @Test
  func testSweptActorWithLiveReferenceRevivesInsteadOfDuplicating() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-revive") {
      $0.bindPort = 2555
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

    let id = VirtualActorID(rawValue: "idle-timeout-revive")
    let actor: TestActor = try await getActorEventually(
      on: system,
      identifiedBy: id,
      dependency: TestActor.None()
    )

    // The sweep fires while `actor` is still held: the entry is downgraded,
    // but the instance survives.
    try await Task.sleep(for: .milliseconds(900))

    // Re-resolving must revive the same instance — never spawn a duplicate.
    let revived: TestActor = try await system.virtualActors.getActor(
      identifiedBy: id,
      dependency: TestActor.None()
    )
    #expect(revived.id == actor.id)
    let found: TestActor = try await node.findActor(identifiedBy: id)
    #expect(found.id == actor.id)
  }

  @Test
  func testRevivedActorIsReWarmedAndSweptAgain() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-rewarm") {
      $0.bindPort = 2556
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

    let id = VirtualActorID(rawValue: "idle-timeout-rewarm")
    var holder: TestActor? = try await getActorEventually(
      on: system,
      identifiedBy: id,
      dependency: TestActor.None()
    )
    weak var weakActor: TestActor?
    weakActor = holder
    let originalID = holder!.id

    // The sweep fires while `holder` keeps the instance alive: the entry is
    // downgraded to a weak hold.
    try await Task.sleep(for: .milliseconds(900))
    #expect(weakActor != nil)

    // A lookup revives the entry: same instance, and the registry holds it
    // strongly again (re-warmed).
    do {
      let revived: TestActor = try await getActorEventually(
        on: system,
        identifiedBy: id,
        dependency: TestActor.None()
      )
      #expect(revived.id == originalID)
    }

    // Dropping the external reference must NOT free the instance — the
    // revived registry hold is the only thing keeping it alive now.
    holder = nil
    #expect(weakActor != nil)

    // The re-warmed entry is swept again after another idle period, and
    // with no references left the instance is freed.
    try await Task.sleep(for: .milliseconds(1200))
    #expect(weakActor == nil)
    await #expect(throws: VirtualNodeError.actorIsMissing) {
      let _: TestActor = try await node.findActor(identifiedBy: id)
    }

    _ = system
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

  @Test
  func testRemoteOnlyActorIsRespawnedAfterSweep() async throws {
    let idle: VirtualNode.IdleTimeoutSettings = .init(
      isEnabled: true,
      cleaningInterval: .milliseconds(200),
      timeout: .milliseconds(500)
    )
    let (ownerSystem, ownerNode) = await ClusterSystem.startVirtualNode(named: "remote-respawn-owner") {
      $0.endpoint = Cluster.Endpoint(host: "127.0.0.1", port: 2560)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(idleTimeoutSettings: idle))
    }
    let clientSystem = await ClusterSystem("remote-respawn-client") {
      $0.endpoint = Cluster.Endpoint(host: "127.0.0.1", port: 2561)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(idleTimeoutSettings: idle))
    }

    ownerSystem.cluster.join(endpoint: clientSystem.cluster.endpoint)
    try await clientSystem.cluster.joined(within: .seconds(3))

    // The client resolves the actor on the owner. A remote reference does
    // not keep the instance alive on the owner.
    let id = VirtualActorID(rawValue: "remote-respawn")
    let first: TestActor = try await getActorEventually(
      on: clientSystem,
      identifiedBy: id,
      dependency: TestActor.None()
    )
    #expect(try await first.ping())

    // The sweep fires on the owner: nothing local holds the actor there,
    // so it is freed — the remote ref alone cannot retain it.
    try await Task.sleep(for: .milliseconds(1200))
    await #expect(throws: VirtualNodeError.actorIsMissing) {
      let _: TestActor = try await ownerNode.findActor(identifiedBy: id)
    }

    // Re-resolving spawns a FRESH instance (crash-equivalent respawn): a new
    // ActorID, fully usable. The virtual guarantee — resolvable by id —
    // holds; continuous liveness was never promised to remote-only users.
    let respawned: TestActor = try await getActorEventually(
      on: clientSystem,
      identifiedBy: id,
      dependency: TestActor.None()
    )
    #expect(respawned.id != first.id)
    #expect(try await respawned.ping())
    _ = ownerSystem
  }

  @Test
  func testLocallyHeldActorRevivesForRemoteLookupAfterSweep() async throws {
    let idle: VirtualNode.IdleTimeoutSettings = .init(
      isEnabled: true,
      cleaningInterval: .milliseconds(200),
      timeout: .milliseconds(500)
    )
    let (ownerSystem, ownerNode) = await ClusterSystem.startVirtualNode(named: "remote-revive-owner") {
      $0.endpoint = Cluster.Endpoint(host: "127.0.0.1", port: 2562)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(idleTimeoutSettings: idle))
    }
    let clientSystem = await ClusterSystem("remote-revive-client") {
      $0.endpoint = Cluster.Endpoint(host: "127.0.0.1", port: 2563)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(idleTimeoutSettings: idle))
    }

    ownerSystem.cluster.join(endpoint: clientSystem.cluster.endpoint)
    try await clientSystem.cluster.joined(within: .seconds(3))

    // The OWNER resolves and holds the actor locally (the way `serve` holds
    // board/documents/supervisor) — a local strong pin.
    let id = VirtualActorID(rawValue: "remote-revive")
    let localHold: TestActor = try await getActorEventually(
      on: ownerSystem,
      identifiedBy: id,
      dependency: TestActor.None()
    )

    // A client resolves the same id remotely.
    let remote: TestActor = try await getActorEventually(
      on: clientSystem,
      identifiedBy: id,
      dependency: TestActor.None()
    )
    #expect(remote.id == localHold.id)

    // The sweep fires: the registry downgrades its hold, but the owner's
    // local pin keeps the instance alive.
    try await Task.sleep(for: .milliseconds(1200))

    // The client re-resolves: the owner's weak slot still resolves, the
    // entry revives, and the client gets the SAME instance — no duplicate,
    // and the original remote ref still works.
    let again: TestActor = try await getActorEventually(
      on: clientSystem,
      identifiedBy: id,
      dependency: TestActor.None()
    )
    #expect(again.id == remote.id)
    #expect(try await remote.ping())

    _ = ownerSystem
    _ = ownerNode
  }

  /// The router singleton discovers the local `VirtualNode` asynchronously
  /// (receptionist listing — early listings can even be dropped before the
  /// subscription is fully wired), so the first resolutions can race it and
  /// fail with `noNodesAvailable`. Retry that one error until the router is
  /// ready; anything else is a real failure and is rethrown at once.
  private func getActorEventually<A: VirtualActor, D: Sendable & Codable>(
    on system: ClusterSystem,
    as type: A.Type = A.self,
    identifiedBy id: VirtualActorID,
    dependency: D
  ) async throws -> A {
    for _ in 0..<100 {
      do {
        return try await system.virtualActors.getActor(identifiedBy: id, dependency: dependency)
      } catch VirtualNodeRouter.Error.noNodesAvailable {
        try await Task.sleep(for: .milliseconds(50))
      }
    }
    // One last attempt — either succeeds or throws the real error.
    return try await system.virtualActors.getActor(identifiedBy: id, dependency: dependency)
  }
}
