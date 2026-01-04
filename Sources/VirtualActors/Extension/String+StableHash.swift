extension String {
  var stableHash: HashKey {
    HashKey.digest(Array(self.utf8))
  }
}
