/// A generic HashRing implementation
struct HashRing<T: Hashable> {
  /// Represents the virtual nodes and their corresponding real nodes
  private var ring: [Int: T] = [:]
  /// The list of real nodes in the ring
  private(set) var nodes: Set<T> = []
  /// Number of virtual nodes per real node
  private var virtualNodes: Int
  
  init(virtualNodes: Int = 100) {
    self.virtualNodes = virtualNodes
  }
  
  /// Adds a node to the ring
  mutating func addNode(_ node: T) {
    guard !nodes.contains(node) else { return }
    
    nodes.insert(node)
    let nodeHash = node.hashValue
    for i in 0..<virtualNodes {
      let virtualNodeHash = HashRing.concatenate(nodeHash: nodeHash, vnode: i)
      ring[virtualNodeHash] = node
    }
  }
  
  /// Removes a node from the ring
  mutating func removeNode(_ node: T) {
    guard nodes.contains(node) else { return }
    
    self.nodes.remove(node)
    let nodeHash = node.hashValue
    for i in 0..<virtualNodes {
      let virtualNodeHash = HashRing.concatenate(nodeHash: nodeHash, vnode: i)
      self.ring.removeValue(forKey: virtualNodeHash)
    }
  }
  
  /// Finds the closest node to the given key in the ring
  func getNode<Key: Hashable>(for key: Key) -> T? {
    guard !self.ring.isEmpty else { return nil }
    
    let sortedKeys = self.ring.keys.sorted()
    let hash = key.hashValue
    
    for k in sortedKeys {
      if hash <= k {
        return self.ring[k]
      }
    }
    
    // Wrap around to the first node in the ring if no node is greater
    return ring[sortedKeys.first!]
  }
  
  /// Combines a node's hash with a virtual node index
  private static func concatenate(nodeHash: Int, vnode: Int) -> Int {
    return nodeHash ^ vnode.hashValue // XOR for simplicity
  }
}
