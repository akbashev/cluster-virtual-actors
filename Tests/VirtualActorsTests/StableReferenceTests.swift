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

  @Test
  func referenceKeptOnAnotherNode_reachesTheActorAfterItIsDeactivated() async throws {
    let first = await ClusterSystem("stable-reference-first") {
      $0.bindPort = 2730
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.installVirtualActors(ClusterVirtualActorsPlugin(replicationFactor: 10))
    }
    let second = await ClusterSystem("stable-reference-second") {
      $0.bindPort = 2731
      $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
      $0.plugins.install(plugin: ClusterSingletonPlugin())
      $0.installVirtualActors(ClusterVirtualActorsPlugin(replicationFactor: 10))
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
    let counter: Counter = try await first.virtualActors.reference(identifiedBy: id, dependency: Counter.None())

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
      let instance: Counter = try await first.virtualActors.getActor(identifiedBy: id, dependency: Counter.None())
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
  }
}
