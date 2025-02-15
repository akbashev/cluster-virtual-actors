public struct VirtualActorID: RawRepresentable, Hashable, Codable, Sendable {
  public var rawValue: String

  public init(rawValue: String) {
    self.rawValue = rawValue
  }
}
