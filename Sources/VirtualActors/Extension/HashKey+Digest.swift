import MurmurHash_Swift

extension HashKey {
  static func digest(_ bytes: [UInt8]) -> HashKey {
    let digest = MurmurHash3.x64_128.digest(bytes)
    return HashKey(
      first: digest.first ?? 0,
      second: digest.count > 1 ? digest[1] : 0
    )
  }
}
