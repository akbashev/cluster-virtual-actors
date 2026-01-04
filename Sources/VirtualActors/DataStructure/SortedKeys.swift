struct SortedKeys<Element: Comparable>: RandomAccessCollection, BidirectionalCollection {
  typealias Index = Int

  private var storage: [Element] = []

  var startIndex: Int { self.storage.startIndex }
  var endIndex: Int { self.storage.endIndex }

  var isEmpty: Bool { self.storage.isEmpty }
  var first: Element? { self.storage.first }

  subscript(position: Int) -> Element {
    self.storage[position]
  }

  func index(after i: Int) -> Int {
    self.storage.index(after: i)
  }

  func index(before i: Int) -> Int {
    self.storage.index(before: i)
  }

  mutating func insert(_ key: Element) {
    let insertIndex = self.index(for: key) ?? self.storage.endIndex
    self.storage.insert(key, at: insertIndex)
  }

  mutating func remove(_ key: Element) {
    guard let index = self.index(of: key) else { return }
    self.storage.remove(at: index)
  }

  func index(for target: Element) -> Int? {
    var low = 0
    var high = self.storage.count

    while low < high {
      let mid = low + (high - low) / 2
      if self.storage[mid] < target {
        low = mid + 1
      } else {
        high = mid
      }
    }

    return low < self.storage.count ? low : nil
  }

  private func index(of target: Element) -> Int? {
    guard let index = self.index(for: target), index < self.storage.count else { return nil }
    return self.storage[index] == target ? index : nil
  }
}
