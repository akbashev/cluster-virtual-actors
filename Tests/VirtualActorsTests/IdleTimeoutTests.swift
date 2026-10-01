import Distributed
import DistributedCluster
import Foundation
import Testing

@testable import VirtualActors

@Suite(.timeLimit(.minutes(1)))
struct IdleTimeoutTests {

  @VirtualActor
  distributed actor TestActor {
    typealias ActorSystem = ClusterSystem

    /// Proves an instance is alive and answering.
    distributed func ping() -> Bool { true }
  }

  /// Mirrors `Agent`: checks into the receptionist on activation and exposes
  /// an `AsyncSequence` of activity fragments, with subscribers parking in
  /// `getNext` until a fragment or a cancellation arrives.
  @VirtualActor
  distributed actor TestSequenceActor: AsyncSequence {
    typealias ActorSystem = ClusterSystem
    typealias Element = String

    struct None: Codable, Sendable {}

    static var receptionKey: DistributedReception.Key<TestSequenceActor> { "test-sequence-actor" }

    private var activityWaiters: [UUID: CheckedContinuation<String?, Never>] = [:]

    init(actorSystem: ClusterSystem) async {
      self.actorSystem = actorSystem
      await actorSystem.receptionist.checkIn(self, with: Self.receptionKey)
    }

    /// Parks the subscriber until a fragment is published or the subscription
    /// is cancelled; mirrors `Agent.getNext(_:)`.
    distributed func getNext(_ subscriber: UUID) async -> String? {
      await withCheckedContinuation { self.activityWaiters[subscriber] = $0 }
    }

    /// Ends one subscription, resuming its parked pull with `nil`.
    distributed func cancel(_ subscriber: UUID) {
      guard let waiter = activityWaiters.removeValue(forKey: subscriber) else { return }
      waiter.resume(returning: nil)
    }

    /// Ends every parked pull at once — the agent-side release.
    distributed func endActivity() {
      let waiters = self.activityWaiters
      self.activityWaiters.removeAll()
      for waiter in waiters.values { waiter.resume(returning: nil) }
    }

    nonisolated func makeAsyncIterator() -> Iterator {
      Iterator(ref: self)
    }

    final class Iterator: AsyncIteratorProtocol {
      private let ref: TestSequenceActor
      private let uuid: UUID = UUID()

      init(ref: TestSequenceActor) {
        self.ref = ref
      }

      func next() async -> String? {
        try? await ref.getNext(uuid)
      }

      deinit {
        let ref = ref
        let uuid = uuid
        Task.detached { try? await ref.cancel(uuid) }
      }
    }
  }

  /// Records the termination of a watched actor.
  distributed actor TestWatcher: LifecycleWatch {
    typealias ActorSystem = ClusterSystem

    private var waiter: CheckedContinuation<ActorID, Never>?

    init(actorSystem: ClusterSystem) {
      self.actorSystem = actorSystem
    }

    distributed func watch(_ actor: TestSequenceActor) {
      self.watchTermination(of: actor)
    }

    /// Parks until the watched actor's termination is delivered.
    distributed func nextTermination() async -> ActorID {
      await withCheckedContinuation { self.waiter = $0 }
    }

    func terminated(actor id: ActorID) async {
      self.waiter?.resume(returning: id)
      self.waiter = nil
    }
  }

  /// An actor with an opinion about deactivation: it refuses the poison pill
  /// while `busy`. Note a pill CAN execute in the middle of a *suspending*
  /// async turn (actor reentrancy) — correctness therefore rests on the
  /// actor checking its own domain state, which is what `busy` models.
  @VirtualActor
  distributed actor TestPickyActor {
    typealias ActorSystem = ClusterSystem

    private var busy = false
    private var workFinished = false
    private var pillsOffered = 0
    private var pillRefusedWhileBusy = false

    func shouldDeactivate() async -> Bool {
      self.pillsOffered += 1
      if self.busy {
        self.pillRefusedWhileBusy = true
        return false
      }
      return true
    }

    distributed func setBusy(_ busy: Bool) {
      self.busy = busy
    }

    /// A turn longer than the idle timeout. The actor marks itself busy for
    /// the duration — the domain signal its pill decision rests on. The turn
    /// suspends on the sleep, so a pill offered mid-turn executes during the
    /// suspension and must refuse.
    distributed func longWork(milliseconds: Int) async {
      self.busy = true
      try? await Task.sleep(for: .milliseconds(milliseconds))
      self.busy = false
      self.workFinished = true
    }

    distributed func didFinishWork() -> Bool { self.workFinished }
    distributed func pillsOfferedCount() -> Int { self.pillsOffered }
    distributed func didRefusePillWhileBusy() -> Bool { self.pillRefusedWhileBusy }
  }

  /// Proves the macro's dependency path: the generated `spawn` casts the
  /// existential dependency and forwards it to `init(actorSystem:dependency:)`.
  @VirtualActor
  distributed actor TestConfiguredActor {
    typealias ActorSystem = ClusterSystem

    struct Dependency: Codable, Sendable {
      let greeting: String
    }

    private let greeting: String

    init(actorSystem: ClusterSystem, dependency: Dependency) {
      self.actorSystem = actorSystem
      self.greeting = dependency.greeting
    }

    distributed func greet() -> String { self.greeting }
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

  /// `deactivate` asks the actor first: a busy actor refuses and stays
  /// resolvable; once idle it agrees and the entry is fully removed — the
  /// immediate, final version of the sweep's question.
  @Test
  func testDeactivateAsksActorBeforeRemoving() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "deactivate-asks") {
      $0.bindPort = 2569
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

    let id = VirtualActorID(rawValue: "deactivate-asks")
    let actor: TestPickyActor = try await getActorEventually(
      on: system,
      identifiedBy: id,
      dependency: TestPickyActor.None()
    )
    try await actor.setBusy(true)

    // Busy: the question is refused, nothing is removed.
    let refused = try await system.virtualActors.deactivate(actor)
    #expect(refused == false)
    let found: TestPickyActor = try await node.findActor(identifiedBy: id)
    #expect(found.id == actor.id)

    // No longer busy: the question is accepted and the entry removed outright.
    try await actor.setBusy(false)
    let accepted = try await system.virtualActors.deactivate(actor)
    #expect(accepted == true)
    await #expect(throws: VirtualNodeError.actorIsMissing) {
      let _: TestPickyActor = try await node.findActor(identifiedBy: id)
    }

    _ = system
  }

  /// Deactivation is idempotent: an id the router never knew (or already
  /// forgot) reports success instead of failing.
  @Test
  func testDeactivateUnknownActorIsIdempotent() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "deactivate-unknown") {
      $0.bindPort = 2570
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    }

    system.cluster.join(endpoint: system.cluster.endpoint)
    try await system.cluster.joined(within: .seconds(3))

    let id = VirtualActorID(rawValue: "deactivate-unknown")
    let actor: TestActor = try await getActorEventually(
      on: system,
      identifiedBy: id,
      dependency: TestActor.None()
    )

    let first = try await system.virtualActors.deactivate(actor)
    #expect(first == true)
    // Second call: the router no longer maps the id — still success.
    let second = try await system.virtualActors.deactivate(actor)
    #expect(second == true)

    _ = node
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

  /// The router may start listening before a local virtual node checks in.
  /// A direct lookup must still resolve the node without requiring callers to
  /// retry the first virtual-actor operation.
  @Test
  func testRouterRefreshesNodesOnCacheMiss() async throws {
    let system = await ClusterSystem("router-refresh") {
      $0.bindPort = 2558
    }
    let router = await VirtualNodeRouter(
      actorSystem: system,
      replicationFactor: 10,
      idleTimeoutSettings: .default
    )
    let node = await VirtualNode(actorSystem: system)

    let actor: TestActor = try await router.getActor(
      identifiedBy: VirtualActorID(rawValue: "router-refresh-actor"),
      dependency: TestActor.None()
    )

    // Simulate the router losing its cached node while the receptionist still
    // knows about it. The next resolution must use the lookup refresh path.
    await router.whenLocal { router in
      await router.terminated(actor: node.id)
    }
    let refreshed: TestActor = try await router.getActor(
      identifiedBy: VirtualActorID(rawValue: "router-refresh-actor"),
      dependency: TestActor.None()
    )

    #expect(try await actor.ping())
    #expect(try await refreshed.ping())
    _ = node
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

  /// Mirrors the real configuration: a `VirtualActor` checked into the
  /// receptionist whose activity sequence is consumed by an open-ended
  /// observer loop (`AgentObserver.watch`). The parked `getNext` pull pins
  /// the actor through the sweep — and cancelling the observer task cannot
  /// release it. Only the agent ending the stream frees the instance, and
  /// the deinit then propagates `terminated` to lifecycle watchers.
  @Test
  func testSequenceObserverPinsSweptActorUntilAgentEndsStream() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-sequence") {
      $0.bindPort = 2557
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

    let watcher = TestWatcher(actorSystem: system)
    let terminationTask = Task { try await watcher.nextTermination() }

    let id = VirtualActorID(rawValue: "idle-timeout-sequence")
    weak var weakActor: TestSequenceActor?
    let originalID: ClusterSystem.ActorID
    do {
      let actor: TestSequenceActor = try await getActorEventually(
        on: system,
        identifiedBy: id,
        dependency: TestSequenceActor.None()
      )
      weakActor = actor
      originalID = actor.id
      try await watcher.watch(actor)
    }

    // The observer: resolve the guest from the receptionist and consume its
    // activity sequence — an open-ended loop parked inside `getNext`. The
    // lookup and loop live inside the task so no strong reference survives
    // in this test's scope after the loop exits.
    let observerTask = Task { () -> Void in
      let guests = await system.receptionist.lookup(TestSequenceActor.receptionKey)
      guard let guest = guests.first else {
        Issue.record("receptionist lookup returned no guests")
        return
      }
      for await _ in guest {}
    }

    // The sweep fires and downgrades the registry's hold, but the instance
    // cannot be freed: the observer's parked pull pins it.
    try await Task.sleep(for: .milliseconds(1500))
    #expect(weakActor != nil)

    // Cancelling the observer task does NOT release the actor either: the
    // loop is parked inside the `getNext` RPC and task cancellation cannot
    // unpark it.
    observerTask.cancel()
    try await Task.sleep(for: .milliseconds(300))
    #expect(weakActor != nil)

    // The release must come from the agent side: ending the parked pulls
    // lets the observer loop exit and drop the last reference.
    do {
      let actor: TestSequenceActor = try await node.findActor(identifiedBy: id)
      try await actor.endActivity()
    }
    _ = await observerTask.value

    // After another idle period the sweep downgrades again and, with no
    // references left, the instance is freed — the receptionist check-in
    // itself never pinned it.
    try await Task.sleep(for: .milliseconds(1500))
    #expect(weakActor == nil)

    // And the deinit propagated `terminated` to the lifecycle watcher.
    #expect(try await terminationTask.value == originalID)

    _ = system
    _ = node
  }

  /// A busy actor refuses the pill: the entry stays warm and the instance
  /// alive across many sweeps. Once it stops being busy the next pill is
  /// swallowed and the instance is freed.
  @Test
  func testRefusedPillKeepsActorWarmUntilSwallowed() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-pill-refuse") {
      $0.bindPort = 2565
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

    let id = VirtualActorID(rawValue: "idle-timeout-pill-refuse")
    weak var weakActor: TestPickyActor?
    do {
      let actor: TestPickyActor = try await getActorEventually(
        on: system,
        identifiedBy: id,
        dependency: TestPickyActor.None()
      )
      weakActor = actor
      try await actor.setBusy(true)
    }

    // Several timeouts pass; the pill is offered but refused, so the
    // registry's strong hold is kept and the actor survives.
    try await Task.sleep(for: .milliseconds(1500))
    #expect(weakActor != nil)

    do {
      let actor: TestPickyActor = try await node.findActor(identifiedBy: id)
      #expect(try await actor.pillsOfferedCount() >= 1)
      try await actor.setBusy(false)
    }

    // No longer busy: the next pill is swallowed, the hold downgrades, and
    // the instance is freed.
    try await Task.sleep(for: .milliseconds(1500))
    #expect(weakActor == nil)

    _ = system
    _ = node
  }

  /// A turn longer than the idle timeout: the pill offered mid-turn executes
  /// during the turn's suspension (actor reentrancy) but is refused on the
  /// actor's own domain state (`busy`). Only after the work completes is a
  /// pill swallowed and the instance released.
  @Test
  func testPillDuringLongTurnIsRefusedUntilWorkCompletes() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-pill-turn") {
      $0.bindPort = 2566
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

    let id = VirtualActorID(rawValue: "idle-timeout-pill-turn")
    weak var weakActor: TestPickyActor?
    do {
      let actor: TestPickyActor = try await getActorEventually(
        on: system,
        identifiedBy: id,
        dependency: TestPickyActor.None()
      )
      weakActor = actor

      // A 1200ms turn — more than twice the 500ms idle timeout.
      let workTask = Task { try? await actor.longWork(milliseconds: 1200) }

      // Mid-turn, past the first timeout: the actor survives because its
      // pill refusals keep the registry's hold warm.
      try await Task.sleep(for: .milliseconds(700))
      #expect(weakActor != nil)

      _ = await workTask.value
      try await Task.sleep(for: .milliseconds(500))

      // At least one pill ran *during* the turn's suspension and was refused
      // on domain state; no pill interrupted the work itself.
      #expect(try await actor.didFinishWork() == true)
      #expect(try await actor.pillsOfferedCount() >= 1)
      #expect(try await actor.didRefusePillWhileBusy() == true)
    }

    // With the work done the next pill was swallowed and the registry
    // downgraded; with the last reference dropped, the instance is freed.
    try await Task.sleep(for: .milliseconds(1200))
    #expect(weakActor == nil)

    _ = system
    _ = node
  }

  /// The macro-generated `spawn` casts the existential dependency and
  /// forwards it to the actor's initializer.
  @Test
  func testSpawnMacroForwardsDependency() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "spawn-macro-dependency") {
      $0.bindPort = 2567
      // For singleton plugin to work we need to choose a leader by having 1 member
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    }

    system.cluster.join(endpoint: system.cluster.endpoint)
    try await system.cluster.joined(within: .seconds(3))

    let actor: TestConfiguredActor = try await getActorEventually(
      on: system,
      identifiedBy: VirtualActorID(rawValue: "spawn-macro-dependency"),
      dependency: TestConfiguredActor.Dependency(greeting: "hello")
    )
    #expect(try await actor.greet() == "hello")

    _ = node
  }

  /// A dependency of the wrong type fails the macro-generated cast —
  /// `VirtualActorError.spawnDependencyTypeMismatch` comes back to the caller.
  @Test
  func testSpawnMacroRejectsMismatchedDependency() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "spawn-macro-mismatch") {
      $0.bindPort = 2568
      // For singleton plugin to work we need to choose a leader by having 1 member
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    }

    system.cluster.join(endpoint: system.cluster.endpoint)
    try await system.cluster.joined(within: .seconds(3))

    await #expect(throws: VirtualActorError.spawnDependencyTypeMismatch) {
      let _: TestConfiguredActor = try await getActorEventually(
        on: system,
        identifiedBy: VirtualActorID(rawValue: "spawn-macro-mismatch"),
        dependency: TestActor.None()
      )
    }

    _ = node
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
