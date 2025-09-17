import CryptoKit
import DistributedCluster
import Foundation

// MARK: - Stable Hash Extensions
extension Cluster.Node {
  var stableHashValue: UInt64 {
    self.endpoint.description.stableHash
  }
}

extension Cluster.Endpoint {
  var stableHashValue: UInt64 {
    self.description.stableHash
  }
}
