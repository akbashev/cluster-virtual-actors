import DistributedCluster
import Testing

@testable import VirtualActors

struct HashRingTests {

  struct Node: Routable {
    let address: Cluster.Node
  }

  @Test
  func testHashRing() {
    // Initialize a HashRing with virtual nodes
    var hashRing = HashRing<Node>(virtualNodesCount: 10)

    // Define some nodes
    let node1 = Node(address: .init(endpoint: .init(host: "host", port: 1), nid: .random()))
    let node2 = Node(address: .init(endpoint: .init(host: "host", port: 2), nid: .random()))
    let node3 = Node(address: .init(endpoint: .init(host: "host", port: 3), nid: .random()))

    hashRing.addNode(node1)
    hashRing.addNode(node2)
    hashRing.addNode(node3)

    #expect(
      hashRing.getNode(for: "key1") != nil,
      "Test 1 Failed: Key should be assigned to a node."
    )

    let testKey = "testKey"
    let assignedNode1 = hashRing.getNode(for: testKey)

    #expect(assignedNode1 != nil, "Test 2 Failed: Key not assigned to any node.")

    // Store the assigned node for comparison
    let initialAssignedNode = assignedNode1!

    hashRing.removeNode(node2)
    let reassignedNode = hashRing.getNode(for: testKey)
    #expect(reassignedNode != nil, "Test 3 Failed: Key not reassigned after node removal.")
    #expect(reassignedNode != node2, "Test 3 Failed: Key reassigned to a removed node.")

    hashRing.addNode(node2)
    let reassignedNodeAfterReaddition = hashRing.getNode(for: testKey)
    #expect(
      reassignedNodeAfterReaddition == initialAssignedNode,
      "Test 4 Failed: Key mapping not consistent after re-adding the node."
    )

    let keys = (1...100).map { "key\($0)" }
    var keyDistribution: [Int: Int] = [:]  // Maps node hash to count

    for key in keys {
      if let node = hashRing.getNode(for: key) {
        keyDistribution[node.hashValue, default: 0] += 1
      }
    }
    #expect(
      keyDistribution.keys.count <= 3,
      "Test 5 Failed: More nodes than expected in the distribution."
    )

    hashRing.removeNode(node1)
    hashRing.removeNode(node2)
    hashRing.removeNode(node3)
    for key in keys {
      #expect(
        hashRing.getNode(for: key) == nil,
        "Test 6 Failed: Keys should not map to any node after all nodes are removed."
      )
    }

    print("All tests passed!")
  }

  @Test
  func testStableHashIgnoresNid() {
    let endpoint = Cluster.Endpoint(host: "host", port: 1)
    let nodeA = Cluster.Node(endpoint: endpoint, nid: .random())
    let nodeB = Cluster.Node(endpoint: endpoint, nid: .random())

    #expect(
      nodeA.stableHashKey == nodeB.stableHashKey,
      "Hash should ignore nid to keep endpoint-only stability."
    )
  }

  @Test
  func testEndpointHashIgnoresSystemName() {
    let endpointA = Cluster.Endpoint(protocol: "sact", systemName: "a", host: "host", port: 1)
    let endpointB = Cluster.Endpoint(protocol: "sact", systemName: "b", host: "host", port: 1)

    #expect(
      endpointA.stableHashKey == endpointB.stableHashKey,
      "Endpoint hash should ignore systemName to match endpoint equality."
    )
  }

  @Test
  func testUInt64KeyRouting() {
    var hashRing = HashRing<Node>(virtualNodesCount: 3)
    let node = Node(address: .init(endpoint: .init(host: "host", port: 1), nid: .random()))
    hashRing.addNode(node)

    let assigned = hashRing.getNode(for: UInt64(42))
    #expect(assigned == node, "UInt64 keys should be routable.")
  }

  @Test
  func testSingleNodeAlwaysSelected() {
    var hashRing = HashRing<Node>(virtualNodesCount: 5)
    let node = Node(address: .init(endpoint: .init(host: "host", port: 1), nid: .random()))
    hashRing.addNode(node)

    let keys = (1...50).map { "key\($0)" }
    for key in keys {
      #expect(hashRing.getNode(for: key) == node, "Single node should own all keys.")
    }
  }

  @Test
  func testIdempotentAddRemove() {
    var hashRing = HashRing<Node>(virtualNodesCount: 7)
    let node = Node(address: .init(endpoint: .init(host: "host", port: 1), nid: .random()))

    hashRing.addNode(node)
    let firstAssigned = hashRing.getNode(for: "key")!

    hashRing.addNode(node)
    let secondAssigned = hashRing.getNode(for: "key")!
    #expect(firstAssigned == secondAssigned, "Adding the same node should be idempotent.")

    hashRing.removeNode(node)
    hashRing.removeNode(node)
    #expect(hashRing.getNode(for: "key") == nil, "Removing the same node should be idempotent.")
  }

  @Test
  func testRingWrapAround() {
    var hashRing = HashRing<Node>(virtualNodesCount: 3)
    let node = Node(address: .init(endpoint: .init(host: "host", port: 1), nid: .random()))
    hashRing.addNode(node)

    let maxKey = HashKey(first: UInt64.max, second: UInt64.max)
    let assigned = hashRing.getNode(for: maxKey)
    #expect(assigned == node, "Keys beyond the last vnode should wrap to the first key.")
  }

  @Test
  func testReaddRestoresMapping() {
    var hashRing = HashRing<Node>(virtualNodesCount: 15)
    let node1 = Node(address: .init(endpoint: .init(host: "host", port: 1), nid: .random()))
    let node2 = Node(address: .init(endpoint: .init(host: "host", port: 2), nid: .random()))
    let node3 = Node(address: .init(endpoint: .init(host: "host", port: 3), nid: .random()))
    let node4 = Node(address: .init(endpoint: .init(host: "host", port: 4), nid: .random()))
    let node5 = Node(address: .init(endpoint: .init(host: "host", port: 5), nid: .random()))

    hashRing.addNode(node1)
    hashRing.addNode(node2)
    hashRing.addNode(node3)
    hashRing.addNode(node4)
    hashRing.addNode(node5)

    let keys = (1...200).map { "key\($0)" }
    var initialMapping: [String: Node] = [:]
    for key in keys {
      if let node = hashRing.getNode(for: key) {
        initialMapping[key] = node
      }
    }

    hashRing.removeNode(node2)
    hashRing.removeNode(node4)
    hashRing.removeNode(node5)
    hashRing.addNode(node5)
    hashRing.addNode(node2)
    hashRing.addNode(node4)

    for key in keys {
      #expect(
        hashRing.getNode(for: key) == initialMapping[key],
        "Key mapping should be restored after re-adding the same node."
      )
    }
  }
}
