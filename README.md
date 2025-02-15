# Cluster System Virtual Actors

Virtual actors framework implementation for Swift [Cluster System](https://github.com/apple/swift-distributed-actors).

## Usage

### Documentation: TODO

1. Install plugins. `ClusterVirtualActorsPlugin` wraps store into singleton, so singleton plugin also should be added (and order is important!).
```swift
let node = await ClusterSystem("simple-node") {
    $0.plugins.install(plugin: ClusterSingletonPlugin())
    $0.plugins.install(plugin: ClusterVirtualActorsPlugin())
}
```

2. Make distributed actor `VirtualActor` and provide `virtualID`:
```swift
distributed actor SomeActor: VirtualActor {
    public static func spawn(on system: DistributedCluster.ClusterSystem, dependency: any Sendable & Codable) async throws -> SomeActor {
        /// A bit of boilerplate to check type until (associated type error)[https://github.com/swiftlang/swift/issues/74769] is fixed
        guard let dependency = dependency as? Dependency else { throw VirtualActorError.spawnDependencyTypeMismatch }
        return SomeActor(actorSystem: system, dependency: dependency)
    }
}
```

3. Call the actor when needed:
```swift
let actor = try await self.actorSystem.virtualActors.getActor(
    identifiedBy: someId,
    dependency: dependency
)
```
