# Cluster Virtual Actors

Virtual actors for Swift, built on [swift-distributed-actors](https://github.com/apple/swift-distributed-actors)—the [Orleans](https://learn.microsoft.com/en-us/dotnet/orleans/) / [Akka cluster sharding](https://doc.akka.io/libraries/akka-core/current/cluster-sharding.html) model, with Swift concurrency at its core.

Model every entity in your system as an actor addressed by ID. You never spawn, place, or reap these actors yourself: the cluster activates them on first use, routes calls to whichever node hosts them, and deactivates them when they go idle—while Swift's ARC does the actual memory bookkeeping.

```swift
// "order-42" may not exist anywhere yet—resolving it activates it,
// on some node in the cluster, and returns a reference to it:
let order: OrderActor = try await system.virtualActors.getActor(
  identifiedBy: VirtualActorID(rawValue: "order-42"),
  dependency: OrderActor.Dependency(repository: db)
)
try await order.add(item: .book, count: 1)
```

## Why virtual actors?

- **An actor per entity, not per pool.** User, order, chat room, ticket—address them by ID and stop managing lifecycle maps by hand. There is always exactly one logical actor per ID.
- **Location transparency.** Callers don't know or care which node hosts the actor. A consistent-hash ring over the cluster's nodes decides placement; references stay valid as actors are deactivated and reactivated.
- **Memory follows actual use.** Idle actors are asked to deactivate, the registry drops its hold, and ARC frees the instance once nothing references it. The next lookup reactivates it—transparently, by ID.
- **Race-free activation.** Concurrent resolutions of the same ID single-flight into one spawn—no duplicate instances, ever.
- **Crash-friendly by construction.** Nothing precious lives in the instance's reachability: if a node or actor dies, the next call simply activates a fresh one on a healthy node. Pair with event sourcing for durable state.

## Quick start

Install the plugins (order matters—the virtual actor store is hosted as a cluster singleton). `configuring(&settings)` also lets references to virtual actors travel between nodes:

```swift
let system = await ClusterSystem("my-node") {
  $0.plugins.install(plugin: ClusterSingletonPlugin())
  let virtualActors = ClusterVirtualActorsPlugin.configuring(&$0)
  $0.plugins.install(plugin: virtualActors)
}
```

Declare your actor. The `@VirtualActor` macro generates the conformance, the spawn boilerplate, and a `None` dependency type for actors that don't need one:

```swift
@VirtualActor
distributed actor OrderActor {
  typealias ActorSystem = ClusterSystem

  struct Dependency: Codable, Sendable {
    let repository: Repository
  }

  private let repository: Repository

  init(actorSystem: ClusterSystem, dependency: Dependency) {
    self.actorSystem = actorSystem
    self.repository = dependency.repository
  }

  distributed func add(item: Item, count: Int) { /* ... */ }
}
```

Resolve and call—from any node:

```swift
let order: OrderActor = try await system.virtualActors.getActor(
  identifiedBy: VirtualActorID(rawValue: "order-42"),
  dependency: OrderActor.Dependency(repository: db)
)
```

The dependency is any `Codable & Sendable` value; it crosses the wire if the actor is activated on a remote node. A wrong dependency type fails the spawn with `VirtualActorError.spawnDependencyTypeMismatch`.

## References

`getActor` returns a *reference* to the actor, not one of its instances. Each call through it reaches whichever instance is running now, and activates one if none is—so a reference stays valid while the actor is deactivated, reactivated, or moved to another node. Keep it, store it, and pass it to actors on other nodes: it keeps working there.

- **Identity is the `VirtualActorID`.** Like a cluster singleton's name, it's how you refer to the actor; each node gets its reference from `getActor`.
- **Equality.** A node hands out one reference per actor, so references from `getActor` on the same node are `==`, and a reference passed to another node stays equal to the original. References obtained independently on different nodes are different handles and aren't `==`—compare the `VirtualActorID`s you resolved them by.
- **`self` is an instance, not a reference.** Inside a virtual actor, `self` is the current instance, which goes away on deactivation. To hand out a reference to itself—subscribing to events, registering somewhere—an actor passes `getActor` of its own ID, not `self`.
- **Cost.** A call through a reference goes to the instance's last known location; only when the instance is gone does it ask the cluster again, and retry the call once.

Need custom spawn logic? Declare your own `spawn(on:dependency:)` and the macro steps aside.

## Deactivation

Enable the idle sweep to reclaim actors nobody has used for a while:

```swift
let virtualActors = ClusterVirtualActorsPlugin.configuring(
  &$0,
  replicationFactor: 100,
  idleTimeoutSettings: .init(
    isEnabled: true,
    cleaningInterval: .seconds(60),
    timeout: .seconds(10 * 60)
  )
)
$0.plugins.install(plugin: virtualActors)
```

How it works, precisely:

1. The sweep notices an actor whose last lookup is older than `timeout`.
2. It asks the actor—on the actor's own executor, via `shouldDeactivate()`. The default answer is `true`, preserving the pure clock-based behavior.
3. On `true`, the registry drops its strong hold. The instance stays alive as long as *anyone* references it (an in-flight call, a subscriber) and is freed by ARC when nothing does.
4. A later lookup *revives* the same instance if it's still alive—never a duplicate—and only spawns a fresh one after the old instance is truly gone.

Override `shouldDeactivate()` to make deactivation a domain decision and a cleanup point—Akka's poison pill, but consensual:

```swift
func shouldDeactivate() async -> Bool {
  guard !isWorking else { return false }   // refuse: sweep asks again after a fresh timeout
  await endActivityStreams()               // cleanup: flush subscribers before going cold
  return true
}
```

An actor that should never be deactivated (a supervisor, a directory) simply refuses unconditionally—func shouldDeactivate() async -> Bool { false }`. There is no separate "always running" flag by design: it would only restate this answer, and a mutable flag would invite flipping it at runtime, which is precisely the domain decision `shouldDeactivate()` already covers.

The question is serialized with the actor's synchronous execution and can never preempt running code. (It *can* run during a suspension of an `async` turn—that's actor reentrancy—so decide from your own state, as above, not from the assumption that no turn is in flight.)

### Explicit deactivation

Don't wait for the sweep—ask directly:

```swift
// Asks shouldDeactivate(), removes the entry on agreement.
// Returns false if the actor refused. Takes a reference, or `self` inside the actor.
try await system.virtualActors.deactivate(order)
```

Unlike the sweep's downgrade this is full removal: in-memory state is lost and the next call through any reference activates a fresh instance. It does not stop a live instance—in-flight work continues and ARC frees the actor once nothing references it. An actor can evict itself the same way (`deactivate(self)`): its own `shouldDeactivate()` is the domain check. There is deliberately no public *unconditional* eviction—the registry forgetting a live actor while the instance keeps running is how duplicates are born; the library reserves that for its own bookkeeping after an instance is actually gone.

## Installation

```swift
dependencies: [
  .package(url: "https://github.com/akbashev/cluster-virtual-actors.git", branch: "main")
]
```

Requires Swift 6.2+, macOS 15 / iOS 18 / tvOS 18 / watchOS 11 (Linux supported), and tracks `main` of [swift-distributed-actors](https://github.com/apple/swift-distributed-actors). References need public call interception in swift-distributed-actors, which isn't released yet.

## See also

- [cluster-event-sourcing](https://github.com/akbashev/cluster-event-sourcing)—@EventSourced` actors with journal-backed state, the natural companion for durability.
- [distributed-actors-showcase](https://github.com/akbashev/distributed-actors-showcase)—example applications.
