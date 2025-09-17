import DistributedCluster
import Foundation
import Testing

@testable import VirtualActors

struct HashRingTests {

  struct Node: Routable {
    let address: Cluster.Node
  }

  @Test
  func testHashRing() {
    // Initialize a HashRing with virtual nodes
    var hashRing = HashRing<Node>(virtualNodes: 10)

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
}
