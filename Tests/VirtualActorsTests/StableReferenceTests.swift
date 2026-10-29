import Distributed
import DistributedCluster
import Testing

@testable import VirtualActors

struct StableReferenceTests {
  distributed actor Counter: VirtualActor {
    typealias ActorSystem = ClusterSystem

    struct None: Codable, Sendable {}

    private var count = 0

    init(actorSystem: ClusterSystem) {
      self.actorSystem = actorSystem
    }

    static func spawn(on actorSystem: ClusterSystem, dependency: any Sendable & Codable) async throws -> Counter {
      Counter(actorSystem: actorSystem)
    }

    distributed func increment() -> Count {
      self.count += 1
      return Count(value: self.count, handledBy: self.id)
    }
  }

  /// A count, and the incarnation that handled the call.
  struct Count: Codable, Sendable, Equatable {
    let value: Int
    let handledBy: ClusterSystem.ActorID
  }

  /// A plain actor on another node that keeps a reference and calls it later.
  distributed actor Keeper {
    typealias ActorSystem = ClusterSystem

    private var kept: Counter?

    init(actorSystem: ClusterSystem) {
      self.actorSystem = actorSystem
    }

    distributed func keep(_ counter: Counter) {
      self.kept = counter
    }

    distributed func increment() async throws -> Count {
      try await self.kept!.increment()
    }
  }

  /// An actor that works on its own, as a bot that polls does: it counts the times it's made.
  distributed actor Worker: VirtualActor {
    typealias ActorSystem = ClusterSystem

    struct None: Codable, Sendable {}

    /// How many workers were made in this process.
    static let made = Made()

    init(actorSystem: ClusterSystem) {
      self.actorSystem = actorSystem
    }

    static func spawn(on actorSystem: ClusterSystem, dependency: any Sendable & Codable) async throws -> Worker {
      await made.add()
      return Worker(actorSystem: actorSystem)
    }
  }

  actor Made {
    private(set) var count = 0

    func add() {
      self.count += 1
    }
  }

  /// Getting an actor makes it when there's none, before anything calls it, and gets the one there
  /// is when there's one: an actor that works on its own runs once it's got.
  @Test
  func gettingAnActorMakesItWhenThereIsNone() async throws {
    let (system, node) = await ClusterSystem.startVirtualNode(named: "get-actor-makes") {
      $0.bindPort = 2734
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      let plugin = ClusterVirtualActorsPlugin.configuring(&$0, replicationFactor: 10)
      $0.plugins.install(plugin: plugin)
    }
    defer { try? system.shutdown() }
    system.cluster.join(endpoint: system.cluster.endpoint)
    try await system.cluster.joined(within: .seconds(3))

    let id = VirtualActorID(rawValue: "worker-1")
    let _: Worker = try await system.virtualActors.getActor(identifiedBy: id, dependency: Worker.None())
    #expect(await Worker.made.count == 1)
    #expect(try await node.actorPlacements().count == 1)
    // Got again, it's the one there is.
    let _: Worker = try await system.virtualActors.getActor(identifiedBy: id, dependency: Worker.None())
    #expect(await Worker.made.count == 1)
  }

  @Test
  func referenceKeptOnAnotherNode_reachesTheActorAfterItIsDeactivated() async throws {
    let first = await ClusterSystem("stable-reference-first") {
      $0.bindPort = 2730
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      let plugin = ClusterVirtualActorsPlugin.configuring(&$0, replicationFactor: 10)
      $0.plugins.install(plugin: plugin)
    }
    let second = await ClusterSystem("stable-reference-second") {
      $0.bindPort = 2731
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      let plugin = ClusterVirtualActorsPlugin.configuring(&$0, replicationFactor: 10)
      $0.plugins.install(plugin: plugin)
    }
    defer {
      try? first.shutdown()
      try? second.shutdown()
    }
    let virtualNode = await VirtualNode(actorSystem: first)
    first.cluster.join(endpoint: first.cluster.endpoint)
    second.cluster.join(endpoint: first.cluster.endpoint)
    try await first.cluster.waitFor(second.cluster.node, .up, within: .seconds(10))

    let id = VirtualActorID(rawValue: "counter-1")
    let counter: Counter = try await first.virtualActors.getActor(identifiedBy: id, dependency: Counter.None())

    // The reference is sent to `second`, which keeps it and calls it from there.
    let keeper = Keeper(actorSystem: second)
    let remoteKeeper = try Keeper.resolve(id: keeper.id, using: first)
    try await remoteKeeper.keep(counter)
    let one = try await remoteKeeper.increment()
    let two = try await remoteKeeper.increment()
    #expect(one.value == 1)
    #expect(two.value == 2)
    #expect(two.handledBy == one.handledBy)
    let firstInstance = one.handledBy
    #expect(try await virtualNode.actorPlacements().map(\.actorID) == [firstInstance])

    // Deactivate the actor, holding it only weakly from here on.
    weak var deactivated: Counter?
    do {
      let instance: Counter = try await first.virtualActors.instance(identifiedBy: id, dependency: Counter.None())
      #expect(instance.id == firstInstance)
      #expect(try await first.virtualActors.deactivate(instance))
      deactivated = instance
    }
    // It's really gone: no node hosts it, and the instance is freed.
    #expect(try await virtualNode.actorPlacements().isEmpty)
    let deadline = ContinuousClock.now + .seconds(2)
    while deactivated != nil, ContinuousClock.now < deadline {
      try await Task.sleep(for: .milliseconds(50))
    }
    #expect(deactivated == nil)

    // The kept reference reaches a new instance: another ActorID, starting over.
    let three = try await remoteKeeper.increment()
    #expect(three.value == 1)
    #expect(three.handledBy != firstInstance)
    #expect(try await virtualNode.actorPlacements().map(\.actorID) == [three.handledBy])

    // A node hands out one reference per actor, and deactivating through it works too.
    let again: Counter = try await first.virtualActors.getActor(identifiedBy: id, dependency: Counter.None())
    #expect(again.id == counter.id)
    #expect(try await first.virtualActors.deactivate(again))
    #expect(try await virtualNode.actorPlacements().isEmpty)
    #expect(try await remoteKeeper.increment().value == 1)
  }

  /// Two nodes' references to one virtual actor are different IDs, which share its virtual ID; an
  /// actor that isn't virtual has none.
  @Test
  func referencesFromTwoNodes_shareTheVirtualActorID() async throws {
    let first = await ClusterSystem("virtual-id-first") {
      $0.bindPort = 2732
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      let plugin = ClusterVirtualActorsPlugin.configuring(&$0, replicationFactor: 10)
      $0.plugins.install(plugin: plugin)
    }
    let second = await ClusterSystem("virtual-id-second") {
      $0.bindPort = 2733
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      let plugin = ClusterVirtualActorsPlugin.configuring(&$0, replicationFactor: 10)
      $0.plugins.install(plugin: plugin)
    }
    defer {
      try? first.shutdown()
      try? second.shutdown()
    }
    let virtualNode = await VirtualNode(actorSystem: first)
    second.cluster.join(endpoint: first.cluster.endpoint)
    try await first.cluster.waitFor(second.cluster.node, .up, within: .seconds(10))

    let id = VirtualActorID(rawValue: "counter-2")
    let fromFirst: Counter = try await first.virtualActors.getActor(identifiedBy: id, dependency: Counter.None())
    let fromSecond: Counter = try await second.virtualActors.getActor(identifiedBy: id, dependency: Counter.None())
    #expect(fromFirst.id != fromSecond.id)
    #expect(fromFirst.id.virtualActorID == id)
    #expect(fromSecond.id.virtualActorID == id)
    // Both reach the one actor.
    #expect(try await fromFirst.increment().value == 1)
    #expect(try await fromSecond.increment().value == 2)
    #expect(Keeper(actorSystem: second).id.virtualActorID == nil)
    _ = virtualNode
  }
}
