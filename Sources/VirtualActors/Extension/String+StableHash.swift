import Crypto // FIXME: Remove in favour of MurmurHash3
import DistributedCluster

extension String {
  var stableHash: UInt64 {
    let inputBytes = Array(self.utf8)
    // FIXME: Move to simple MurmurHash3
    // Was a bit lazy a took first thing, just need a simple non-cryptographic hash function
    return
      SHA256
      .hash(data: inputBytes)
      .prefix(8)
      .reduce(0) { ($0 << 8) | UInt64($1) }
  }
}
