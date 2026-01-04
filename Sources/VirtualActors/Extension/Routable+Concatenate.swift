extension Routable {
  func concatenate(vnode: UInt64) -> HashKey {
    let nodeKey = self.address.stableHashKey
    var bytes: [UInt8] = []
    bytes.reserveCapacity(24)
    bytes.append(contentsOf: nodeKey.first.bigEndianBytes)
    bytes.append(contentsOf: nodeKey.second.bigEndianBytes)
    bytes.append(contentsOf: vnode.bigEndianBytes)
    return HashKey.digest(bytes)
  }
}

extension FixedWidthInteger {
  var bigEndianBytes: [UInt8] {
    withUnsafeBytes(of: self.bigEndian) { Array($0) }
  }
}
