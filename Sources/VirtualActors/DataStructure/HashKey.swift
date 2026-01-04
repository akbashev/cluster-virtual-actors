struct HashKey: Hashable, Comparable {
  let first: UInt64
  let second: UInt64

  init(first: UInt64, second: UInt64) {
    self.first = first
    self.second = second
  }

  static func < (lhs: HashKey, rhs: HashKey) -> Bool {
    if lhs.first != rhs.first { return lhs.first < rhs.first }
    return lhs.second < rhs.second
  }
}
