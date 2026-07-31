import Distributed
import DistributedCluster

/// Adds the ``VirtualActor`` conformance and generates the
/// ``VirtualActor/spawn(on:dependency:)`` boilerplate (unless the actor
/// declares its own `spawn`):
///
/// - With a nested `Dependency` type and `init(actorSystem:dependency:)`,
///   the generated `spawn` casts the existential dependency — the protocol
///   can't use an associated type, see the comment on ``VirtualActor`` — and
///   forwards it to the initializer.
/// - Without one, the generated `spawn` ignores the dependency and calls
///   `init(actorSystem:)` (explicit or synthesized), and the macro also adds
///   a `None` type to pass at the call site (unless the actor declares one).
///
/// `async`/`throws` initializers are matched by the generated call.
@attached(member, names: named(spawn), named(None))
@attached(extension, conformances: VirtualActor)
public macro VirtualActor() = #externalMacro(module: "VirtualActorsMacros", type: "VirtualActorMacro")

public protocol VirtualActor: DistributedActor, Codable where ActorSystem == ClusterSystem {
  /// Unfortunately due to (associated type error)[https://github.com/swiftlang/swift/issues/74769] we can't just make it associated type
  ///  associatedtype Dependency: VirtualActorDependency
  static func spawn(on actorSystem: ClusterSystem, dependency: any Sendable & Codable) async throws -> Self

  /// Asked by the node's idle sweep before it drops the registry's strong
  /// hold on the actor. The question runs *isolated on the actor*, serialized
  /// with its synchronous execution — it can never preempt running code.
  /// Note however that it CAN execute in the middle of a *suspending* async
  /// turn (actor reentrancy: a turn awaiting I/O frees the executor), so the
  /// decision must rest on the actor's own domain state — e.g. a "working"
  /// flag — not on the assumption that no turn is in flight. Answering `true`
  /// is also the actor's chance to clean up before deactivation (end streams,
  /// flush waiters).
  ///
  /// - Returns: `true` to accept deactivation — the registry downgrades its
  ///   hold and the instance is freed once no references remain; `false` to
  ///   stay warm — the idle clock is bumped and the question is asked again
  ///   only after a full new timeout.
  ///
  /// There is deliberately no "always running" flag: an actor that is never
  /// subject to the idle lifecycle simply answers `false` unconditionally
  /// (`func shouldDeactivate() async -> Bool { false }`). A static flag would
  /// invite flipping it at runtime — which is exactly the dynamic,
  /// domain-state decision this method exists for.
  func shouldDeactivate() async -> Bool
}

public enum VirtualActorError: Error, Codable, Sendable {
  case spawnDependencyTypeMismatch
}

extension VirtualActor {
  /// The default answer: deactivate immediately. Plain actors keep the
  /// previous behavior — a pure clock-based sweep — and don't need to
  /// implement ``shouldDeactivate()`` themselves.
  public func shouldDeactivate() async -> Bool { true }

  nonisolated func resign() async throws {
    try await self.actorSystem.virtualActors.cleanActor(self)
  }
}
