import Distributed
import DistributedCluster
import Foundation

/// What a reference to a virtual actor carries in its ``ActorID``, so that any node can reach the
/// actor — activating it if needed — instead of one incarnation of it.
struct VirtualActorAddress: Codable, Sendable, Hashable {
  let id: VirtualActorID
  /// The actor's type and its dependency, to activate it on any node.
  let actorType: String
  let dependencyType: String
  let dependency: Data

  init<A: VirtualActor, D: Codable & Sendable>(id: VirtualActorID, actorType: A.Type, dependency: D) throws {
    guard let actorType = _mangledTypeName(A.self), let dependencyType = _mangledTypeName(D.self) else {
      throw VirtualActorReferenceError.unnamedType
    }
    self.id = id
    self.actorType = actorType
    self.dependencyType = dependencyType
    self.dependency = try JSONEncoder().encode(dependency)
  }

  /// The current instance's ID, activated through `router` if it isn't running.
  func instanceID(using router: VirtualNodeRouter) async throws -> ClusterSystem.ActorID {
    guard let actorType = _typeByName(self.actorType) as? any VirtualActor.Type,
      let dependencyType = _typeByName(self.dependencyType) as? any (Codable & Sendable).Type
    else {
      throw VirtualActorReferenceError.unknownType
    }
    let dependency = try JSONDecoder().decode(dependencyType, from: self.dependency)
    return try await Self.instanceID(of: actorType, identifiedBy: self.id, dependency: dependency, using: router)
  }

  private static func instanceID<A: VirtualActor, D: Codable & Sendable>(
    of actorType: A.Type,
    identifiedBy id: VirtualActorID,
    dependency: D,
    using router: VirtualNodeRouter
  ) async throws -> ClusterSystem.ActorID {
    let actor: A = try await router.getActor(identifiedBy: id, dependency: dependency)
    return actor.id
  }
}

public enum VirtualActorReferenceError: Error, Codable, Sendable {
  /// The actor or dependency type has no runtime name, e.g. because it's private.
  case unnamedType
  /// This node doesn't know the actor or dependency type of a reference it received.
  case unknownType
}

extension ActorMetadataKeys {
  /// Marks a reference to a virtual actor, made by ``ClusterVirtualActorsPlugin/reference(identifiedBy:dependency:)``.
  var virtualActor: Key<VirtualActorAddress> { "$virtualActor" }
}

/// Delivers a reference's calls to the virtual actor's current instance, activating it if needed.
///
/// The instance's location is cached. A call that finds the instance gone (a dead letter, so it
/// didn't run) asks for the location again and is retried once.
struct VirtualActorInterceptor: RemoteCallInterceptor {
  let plugin: ClusterVirtualActorsPlugin
  let address: VirtualActorAddress

  func interceptRemoteCall<Act, Err, Res>(
    on actor: Act,
    target: RemoteCallTarget,
    invocation: inout ClusterSystem.InvocationEncoder,
    throwing: Err.Type,
    returning: Res.Type
  ) async throws -> Res
  where Act: DistributedActor, Act.ID == ActorID, Err: Error, Res: Codable {
    let (system, location) = try await self.plugin.location(of: self.address)
    do {
      return try await system.forwardCall(
        to: location,
        as: Act.self,
        target: target,
        invocation: &invocation,
        throwing: throwing,
        returning: returning
      )
    } catch is DeadLetterError {
      await self.plugin.forget(self.address.id, at: location)
      let (system, location) = try await self.plugin.location(of: self.address)
      return try await system.forwardCall(
        to: location,
        as: Act.self,
        target: target,
        invocation: &invocation,
        throwing: throwing,
        returning: returning
      )
    }
  }

  func interceptRemoteCallVoid<Act, Err>(
    on actor: Act,
    target: RemoteCallTarget,
    invocation: inout ClusterSystem.InvocationEncoder,
    throwing: Err.Type
  ) async throws
  where Act: DistributedActor, Act.ID == ActorID, Err: Error {
    let (system, location) = try await self.plugin.location(of: self.address)
    do {
      try await system.forwardCallVoid(to: location, as: Act.self, target: target, invocation: &invocation, throwing: throwing)
    } catch is DeadLetterError {
      await self.plugin.forget(self.address.id, at: location)
      let (system, location) = try await self.plugin.location(of: self.address)
      try await system.forwardCallVoid(to: location, as: Act.self, target: target, invocation: &invocation, throwing: throwing)
    }
  }
}
