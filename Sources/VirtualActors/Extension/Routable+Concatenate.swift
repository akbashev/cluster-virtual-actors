import Crypto
import Foundation

extension FixedWidthInteger {
  var bigEndianBytes: [UInt8] {
    withUnsafeBytes(of: self.bigEndian) { Array($0) }
  }
}

extension Routable {
  func concatenate(vnode: UInt64) -> UInt64 {
    let inputBytes = address.stableHashValue.bigEndianBytes + vnode.bigEndianBytes
    return SHA256.hash(data: inputBytes)
      .prefix(8)
      .reduce(0) { ($0 << 8) | UInt64($1) }
  }
}
