import DistributedCluster

// Small helper, not sure if it will be needed, but let's play
extension ClusterSystem {
  public static func startVirtualNode(
    named name: String,
    configuredWith configureSettings: @Sendable (inout ClusterSystemSettings) -> Void = { _ in () }
  ) async -> (ClusterSystem, VirtualNode) {
    let system = await ClusterSystem(name) { settings in
      configureSettings(&settings)
    }
    let node = await VirtualNode(actorSystem: system)
    return (system, node)
  }
}
