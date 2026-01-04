import DistributedCluster

// MARK: - Stable Hash Extensions
extension Cluster.Node {
  var stableHashKey: HashKey {
    self.endpoint.stableHashKey
  }
}

extension Cluster.Endpoint {
  var stableHashKey: HashKey {
    HashKey.digest(self.stableHashBytes)
  }

  fileprivate var stableHashBytes: [UInt8] {
    var bytes: [UInt8] = []
    bytes.reserveCapacity(self.`protocol`.utf8.count + 1 + host.utf8.count + 1 + 4)
    bytes.append(contentsOf: self.`protocol`.utf8)
    bytes.append(0)
    bytes.append(contentsOf: self.host.utf8)
    bytes.append(0)
    bytes.append(contentsOf: UInt32(self.port).bigEndianBytes)
    return bytes
  }
}
