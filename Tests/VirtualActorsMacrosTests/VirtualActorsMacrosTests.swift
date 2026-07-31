import SwiftSyntax
import SwiftSyntaxBuilder
import SwiftSyntaxMacros
import SwiftSyntaxMacrosTestSupport
import Testing
import VirtualActorsMacros

struct VirtualActorsMacrosTests {

  private let testMacros: [String: Macro.Type] = [
    "VirtualActor": VirtualActorMacro.self
  ]

  @Test
  func testActorWithoutDependency() throws {
    assertMacroExpansion(
      """
      @VirtualActor
      distributed actor Greeter {
        init(actorSystem: ClusterSystem) {
          self.actorSystem = actorSystem
        }
      }
      """,
      expandedSource: """
        distributed actor Greeter {
          init(actorSystem: ClusterSystem) {
            self.actorSystem = actorSystem
          }

            public static func spawn(
              on actorSystem: ClusterSystem,
              dependency: any Sendable & Codable
            ) async throws -> Self {
              return Self(actorSystem: actorSystem)
            }

            public struct None: Codable, Sendable {
            }
        }

        extension Greeter: VirtualActor {
        }
        """,
      macros: testMacros
    )
  }

  @Test
  func testActorWithAsyncInit() throws {
    assertMacroExpansion(
      """
      @VirtualActor
      distributed actor Greeter {
        init(actorSystem: ClusterSystem) async {
          self.actorSystem = actorSystem
        }
      }
      """,
      expandedSource: """
        distributed actor Greeter {
          init(actorSystem: ClusterSystem) async {
            self.actorSystem = actorSystem
          }

            public static func spawn(
              on actorSystem: ClusterSystem,
              dependency: any Sendable & Codable
            ) async throws -> Self {
              return await Self(actorSystem: actorSystem)
            }

            public struct None: Codable, Sendable {
            }
        }

        extension Greeter: VirtualActor {
        }
        """,
      macros: testMacros
    )
  }

  @Test
  func testActorWithDependency() throws {
    assertMacroExpansion(
      """
      @VirtualActor
      distributed actor Greeter {
        struct Dependency: Codable, Sendable {
          let greeting: String
        }
        init(actorSystem: ClusterSystem, dependency: Dependency) {
          self.actorSystem = actorSystem
        }
      }
      """,
      expandedSource: """
        distributed actor Greeter {
          struct Dependency: Codable, Sendable {
            let greeting: String
          }
          init(actorSystem: ClusterSystem, dependency: Dependency) {
            self.actorSystem = actorSystem
          }

            public static func spawn(
              on actorSystem: ClusterSystem,
              dependency: any Sendable & Codable
            ) async throws -> Self {
              guard let dependency = dependency as? Dependency else {
                throw VirtualActorError.spawnDependencyTypeMismatch
              }
              return Self(actorSystem: actorSystem, dependency: dependency)
            }
        }

        extension Greeter: VirtualActor {
        }
        """,
      macros: testMacros
    )
  }

  @Test
  func testActorWithCustomSpawnKeepsIt() throws {
    assertMacroExpansion(
      """
      @VirtualActor
      distributed actor Greeter {
        struct None: Codable, Sendable {}
        init(actorSystem: ClusterSystem) {
          self.actorSystem = actorSystem
        }
        static func spawn(on actorSystem: ClusterSystem, dependency: any Sendable & Codable) async throws -> Greeter {
          Greeter(actorSystem: actorSystem)
        }
      }
      """,
      expandedSource: """
        distributed actor Greeter {
          struct None: Codable, Sendable {}
          init(actorSystem: ClusterSystem) {
            self.actorSystem = actorSystem
          }
          static func spawn(on actorSystem: ClusterSystem, dependency: any Sendable & Codable) async throws -> Greeter {
            Greeter(actorSystem: actorSystem)
          }
        }

        extension Greeter: VirtualActor {
        }
        """,
      macros: testMacros
    )
  }
}
