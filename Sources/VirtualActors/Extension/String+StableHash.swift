import Crypto
import DistributedCluster

extension String {
  var stableHash: UInt64 {
    let inputBytes = Array(self.utf8)
    return
      SHA256
      .hash(data: inputBytes)
      .prefix(8)
      .reduce(0) { ($0 << 8) | UInt64($1) }
  }
}
