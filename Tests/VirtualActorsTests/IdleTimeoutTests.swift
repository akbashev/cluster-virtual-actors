import Distributed
import DistributedCluster
import Testing

@testable import VirtualActors

struct IdleTimeoutTests {

  distributed actor TestActor: VirtualActor {
    typealias ActorSystem = ClusterSystem

    struct Dependency: Codable, Sendable {
      let value: Int
    }

    let dependency: Dependency

    init(actorSystem: ClusterSystem, dependency: Dependency) {
      self.actorSystem = actorSystem
      self.dependency = dependency
    }

    static func spawn(
      on actorSystem: ClusterSystem,
      dependency: any Sendable & Codable
    ) async throws -> TestActor {
      guard let dependency = dependency as? Dependency else {
        throw VirtualActorError.spawnDependencyTypeMismatch
      }
      return TestActor(actorSystem: actorSystem, dependency: dependency)
    }
  }

  @Test
  func testIdleTimeoutRemovesActors() async throws {
    let (_, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-remove")
    let settings = VirtualNode.IdleTimeoutSettings(
      isEnabled: true,
      cleaningInterval: .milliseconds(200),
      timeout: .milliseconds(500)
    )
    try await node.updateTimeoutSettings(settings)

    let id = VirtualActorID(rawValue: "idle-timeout-remove")
    let _: TestActor = try await node.spawnActor(
      identifiedBy: id,
      dependency: TestActor.Dependency(value: 1)
    )

    try await Task.sleep(for: .milliseconds(900))

    await #expect(throws: VirtualNodeError.actorIsMissing) {
      let _: TestActor = try await node.findActor(identifiedBy: id)
    }
  }

  @Test
  func testMarkAsActiveKeepsActorAlive() async throws {
    let (_, node) = await ClusterSystem.startVirtualNode(named: "idle-timeout-keepalive")
    let settings = VirtualNode.IdleTimeoutSettings(
      isEnabled: true,
      cleaningInterval: .milliseconds(200),
      timeout: .milliseconds(600)
    )
    try await node.updateTimeoutSettings(settings)

    let id = VirtualActorID(rawValue: "idle-timeout-keepalive")
    let _: TestActor = try await node.spawnActor(
      identifiedBy: id,
      dependency: TestActor.Dependency(value: 2)
    )

    try await Task.sleep(for: .milliseconds(250))
    try await node.markActorAsActive(identifiedBy: id)
    try await Task.sleep(for: .milliseconds(250))

    let _: TestActor = try await node.findActor(identifiedBy: id)
  }
}
