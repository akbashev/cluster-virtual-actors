import DistributedCluster

protocol Routable: Hashable {
  nonisolated var address: Cluster.Node { get }
}

/// A generic HashRing implementation
struct HashRing<T: Routable> {
  /// Represents the virtual nodes and their corresponding real nodes
  private var ring: [HashKey: [T]] = [:]
  /// A sorted array of keys in the ring for efficient lookup
  // TODO: If churn gets high, consider a balanced tree to keep inserts/removals O(log n).
  private var sortedKeys = SortedKeys<HashKey>()
  /// The list of real nodes in the ring
  private(set) var nodes: Set<T> = []
  /// Cached vnode hashes per node to avoid recomputing on removal
  private var nodeHashes: [T: [HashKey]] = [:]
  /// Number of virtual nodes per real node
  private var virtualNodesCount: UInt64

  init(virtualNodesCount: UInt64 = 150) {
    self.virtualNodesCount = virtualNodesCount
  }

  /// Adds a node to the ring
  mutating func addNode(_ node: T) {
    guard !self.nodes.contains(node) else { return }

    self.nodes.insert(node)
    let hashes = self.computeVNodeHashes(for: node)
    self.nodeHashes[node] = hashes

    for virtualNodeHash in hashes {
      if self.ring[virtualNodeHash] == nil {
        self.ring[virtualNodeHash] = [node]
        self.sortedKeys.insert(virtualNodeHash)
      } else {
        self.insertNode(node, for: virtualNodeHash)
      }
    }
  }

  /// Removes a node from the ring
  mutating func removeNode(_ node: T) {
    guard self.nodes.contains(node) else { return }

    self.nodes.remove(node)
    let hashes = self.nodeHashes[node] ?? self.computeVNodeHashes(for: node)
    self.nodeHashes.removeValue(forKey: node)

    for virtualNodeHash in hashes {
      guard var bucket = self.ring[virtualNodeHash] else { continue }
      if let index = bucket.firstIndex(of: node) {
        bucket.remove(at: index)
        if bucket.isEmpty {
          self.ring.removeValue(forKey: virtualNodeHash)
          self.sortedKeys.remove(virtualNodeHash)
        } else {
          self.ring[virtualNodeHash] = bucket
        }
      }
    }
  }

  /// Finds the closest node to the given key in the ring
  func getNode(for key: String) -> T? {
    self.getNode(for: key.stableHash)
  }

  /// Finds the closest node to the given key in the ring
  func getNode(for key: UInt64) -> T? {
    self.getNode(for: HashKey(first: 0, second: key))
  }

  /// Finds the closest node to the given key in the ring
  func getNode(for key: HashKey) -> T? {
    guard !self.ring.isEmpty else { return nil }
    guard let closestKeyIndex = self.sortedKeys.index(for: key) else {
      guard let firstKey = self.sortedKeys.first else { return nil }
      return self.ring[firstKey]?.first
    }

    return self.ring[self.sortedKeys[closestKeyIndex]]?.first
  }
}

extension HashRing {

  private mutating func insertNode(_ node: T, for key: HashKey) {
    guard var bucket = self.ring[key] else { return }
    guard !bucket.contains(node) else { return }
    let insertIndex = self.insertionIndex(for: node, in: bucket)
    bucket.insert(node, at: insertIndex)
    self.ring[key] = bucket
  }

  private func insertionIndex(for node: T, in bucket: [T]) -> Int {
    var low = 0
    var high = bucket.count

    while low < high {
      let mid = low + (high - low) / 2
      if bucket[mid].address < node.address {
        low = mid + 1
      } else {
        high = mid
      }
    }

    return low
  }

  private func computeVNodeHashes(for node: T) -> [HashKey] {
    // Precompute vnode hashes so removal doesn't need to recompute them.
    var hashes: [HashKey] = []
    hashes.reserveCapacity(Int(self.virtualNodesCount))
    for i in 0..<self.virtualNodesCount {
      hashes.append(node.concatenate(vnode: i))
    }
    return hashes
  }
}
