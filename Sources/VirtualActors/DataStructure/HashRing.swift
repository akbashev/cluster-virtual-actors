import DistributedCluster

protocol Routable: Hashable {
  nonisolated var address: Cluster.Node { get }
}

/// A generic HashRing implementation
// FIXME: Test more, check different edge-cases.
struct HashRing<T: Routable> {
  /// Represents the virtual nodes and their corresponding real nodes
  private var ring: [UInt64: T] = [:]
  /// A sorted array of keys in the ring for efficient lookup
  // TODO: We need to sort every time, probably some tree or binary insert is better here?
  private var sortedKeys: [UInt64] = []
  /// The list of real nodes in the ring
  private(set) var nodes: Set<T> = []
  /// Number of virtual nodes per real node
  private var virtualNodes: UInt64

  init(virtualNodes: UInt64 = 150) {
    self.virtualNodes = max(virtualNodes, 50)
  }

  /// Adds a node to the ring
  mutating func addNode(_ node: T) {
    guard !self.nodes.contains(node) else { return }

    self.nodes.insert(node)

    for i in 0..<self.virtualNodes {
      let virtualNodeHash = node.concatenate(vnode: i)
      self.ring[virtualNodeHash] = node
    }

    self.sortedKeys = self.ring.keys.sorted()
  }

  /// Removes a node from the ring
  mutating func removeNode(_ node: T) {
    guard self.nodes.contains(node) else { return }

    self.nodes.remove(node)

    for i in 0..<self.virtualNodes {
      let virtualNodeHash = node.concatenate(vnode: i)
      self.ring.removeValue(forKey: virtualNodeHash)
    }

    self.sortedKeys = self.ring.keys.sorted()
  }

  /// Finds the closest node to the given key in the ring
  // TODO: Probably add generic StableHashable? protocol and conform String, UUID, Int, etc... to it.
  func getNode(for key: String) -> T? {
    guard !self.ring.isEmpty else { return nil }
    guard let closestKeyIndex = self.sortedKeys.index(for: key.stableHash) else {
      // we already checked if not empty
      return self.ring[sortedKeys[0]]
    }

    return self.ring[sortedKeys[closestKeyIndex]]
  }
}

extension Array where Element: Comparable {
  fileprivate func index(for target: Element) -> Index? {
    var low = startIndex
    var high = endIndex

    while low < high {
      let mid = low + (high - low) / 2
      if self[mid] < target {
        low = mid + 1
      } else {
        high = mid
      }
    }

    return low < count ? low : nil
  }
}
