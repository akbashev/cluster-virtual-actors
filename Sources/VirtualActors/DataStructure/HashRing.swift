import DistributedCluster

protocol Routable: Hashable {
  nonisolated var address: Cluster.Node { get }
}

/// A generic HashRing implementation
// FIXME: Test more, check different edge-cases
struct HashRing<T: Routable> {
  /// Represents the virtual nodes and their corresponding real nodes
  private var ring: [UInt64: T] = [:]
  /// A sorted array of keys in the ring for efficient lookup,
  /// in combination with binarySearch should give a better perfomance.
  private var sortedKeys: [UInt64] = []
  /// The list of real nodes in the ring
  private(set) var nodes: Set<T> = []
  /// Number of virtual nodes per real node
  private var virtualNodes: Int

  init(virtualNodes: Int = 100) {
    self.virtualNodes = virtualNodes
  }

  /// Adds a node to the ring
  mutating func addNode(_ node: T) {
    guard !self.nodes.contains(node) else { return }

    self.nodes.insert(node)
    let nodeHash = node.address.stableHashValue
    for i in 0..<virtualNodes {
      let virtualNodeHash = HashRing.concatenate(nodeHash: nodeHash, vnode: i)
      self.ring[virtualNodeHash] = node
    }
    self.sortedKeys = self.ring.keys.sorted()
  }

  /// Removes a node from the ring
  mutating func removeNode(_ node: T) {
    guard self.nodes.contains(node) else { return }

    self.nodes.remove(node)
    let nodeHash = node.address.stableHashValue
    for i in 0..<virtualNodes {
      let virtualNodeHash = HashRing.concatenate(nodeHash: nodeHash, vnode: i)
      self.ring.removeValue(forKey: virtualNodeHash)
    }
    self.sortedKeys = self.ring.keys.sorted()
  }

  /// Finds the closest node to the given key in the ring
  func getNode(for key: String) -> T? {
    guard !self.ring.isEmpty else { return nil }
    guard let closestKey = self.sortedKeys.binarySearch(predicate: { $0 >= key.stableHash }) else {
      return self.ring[self.sortedKeys.first!]
    }
    return self.ring[closestKey]
  }

  /// Combines a node's hash with a virtual node index
  private static func concatenate(nodeHash: UInt64, vnode: Int) -> UInt64 {
    var x = nodeHash &* 0x9e37_79b9_7f4a_7c15
    x ^= UInt64(vnode) &* 0x9e37_79b9_7f4a_7c15
    return x
  }
}

/// Simple binary search for performance.
extension Array where Element: Comparable {
  fileprivate func binarySearch(predicate: (Element) -> Bool) -> Element? {
    var low = 0
    var high = count - 1
    while low <= high {
      let mid = (low + high) / 2
      if predicate(self[mid]) {
        high = mid - 1
      } else {
        low = mid + 1
      }
    }
    if low < count, predicate(self[low]) {
      return self[low]  // return element instead of index
    }
    return nil
  }
}
