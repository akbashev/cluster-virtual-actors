# Cluster Virtual actors

Virtual actors framework implementation for Swift distributed [Cluster System](https://github.com/apple/swift-distributed-actors).

## Usage

### Documentation: TODO

1. Install plugins. `ClusterJournalPlugin` wraps store into singleton, so singleton plugin also should be added (and order is important!).
```swift
let node = await ClusterSystem("simple-node") {
    $0.plugins.install(plugin: ClusterSingletonPlugin())
    $0.plugins.install(plugin: ClusterVirtualActorsPlugin())
}
```

2. Make distributed actor `VirtualActor` and provide `virtualID`:
```swift
distributed actor SomeActor: EventSourced {
    distributed var virtualID: VirtualActorID { "some-actor" }
}
```

3. Call the actor when needed:
```swift
let actor = try await self.actorSystem.virtualActors.getActor(
  withId: someId
) {
  // If actor is new—it should be created.
  await SomeActor(
    actorSystem: actorSystem,
    id: someId
  )
}
```
