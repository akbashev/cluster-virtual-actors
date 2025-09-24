import Crypto
import DistributedCluster
import Foundation

extension String {
  var stableHash: UInt64 {
    SHA256
      .hash(data: Data(self.utf8))
      .withUnsafeBytes { rawBuffer in
        rawBuffer.load(as: UInt64.self).bigEndian
      }
  }
}
