import SwiftCompilerPlugin
import SwiftSyntax
import SwiftSyntaxBuilder
import SwiftSyntaxMacros

/// Generates the `VirtualActor` conformance and, unless the actor declares
/// its own, the `spawn(on:dependency:)` boilerplate. Two shapes:
///
/// - The actor has a nested `Dependency` type and an
///   `init(actorSystem:dependency:)` — the generated `spawn` casts the
///   existential dependency (the protocol can't use an associated type, see
///   https://github.com/swiftlang/swift/issues/74769) and forwards it.
/// - Otherwise — the generated `spawn` ignores the dependency and calls
///   `init(actorSystem:)` (explicit or synthesized), and the macro also adds
///   a `None` type to pass at the call site, unless the actor declares one.
///
/// `async`/`throws` on the matched initializer are mirrored in the generated
/// call.
public struct VirtualActorMacro: MemberMacro, ExtensionMacro {
  public static func expansion(
    of node: AttributeSyntax,
    providingMembersOf declaration: some DeclGroupSyntax,
    conformingTo protocols: [TypeSyntax],
    in context: some MacroExpansionContext
  ) throws -> [DeclSyntax] {
    guard let actor = declaration.as(ActorDeclSyntax.self) else { return [] }
    let members = actor.memberBlock.members.map(\.decl)

    // An actor with its own `spawn` keeps it — the macro only fills the gap.
    let hasSpawn = members.contains { decl in
      guard let function = decl.as(FunctionDeclSyntax.self) else { return false }
      return function.name.text == "spawn"
    }

    let hasDependency = members.contains { decl in
      if let typeAlias = decl.as(TypeAliasDeclSyntax.self) { return typeAlias.name.text == "Dependency" }
      if let structDecl = decl.as(StructDeclSyntax.self) { return structDecl.name.text == "Dependency" }
      if let classDecl = decl.as(ClassDeclSyntax.self) { return classDecl.name.text == "Dependency" }
      if let enumDecl = decl.as(EnumDeclSyntax.self) { return enumDecl.name.text == "Dependency" }
      return false
    }

    // Match the effects of the conventional initializer so `async`/`throws`
    // initializers are called correctly. An actor without an explicit
    // initializer gets the synthesized `init(actorSystem:)` — sync and
    // non-throwing, which is what the empty keywords produce.
    let expectedLabels = hasDependency ? ["actorSystem", "dependency"] : ["actorSystem"]
    var tryKeyword = ""
    var awaitKeyword = ""
    for decl in members {
      guard let initializer = decl.as(InitializerDeclSyntax.self) else { continue }
      let labels = initializer.signature.parameterClause.parameters.map { $0.firstName.text }
      guard labels == expectedLabels else { continue }
      if initializer.signature.effectSpecifiers?.asyncSpecifier != nil { awaitKeyword = "await " }
      if initializer.signature.effectSpecifiers?.throwsClause != nil { tryKeyword = "try " }
      break
    }

    var generated: [DeclSyntax] = []
    if !hasSpawn {
      let body: String
      if hasDependency {
        body = """
          public static func spawn(
            on actorSystem: ClusterSystem,
            dependency: any Sendable & Codable
          ) async throws -> Self {
            guard let dependency = dependency as? Dependency else {
              throw VirtualActorError.spawnDependencyTypeMismatch
            }
            return \(tryKeyword)\(awaitKeyword)Self(actorSystem: actorSystem, dependency: dependency)
          }
          """
      } else {
        body = """
          public static func spawn(
            on actorSystem: ClusterSystem,
            dependency: any Sendable & Codable
          ) async throws -> Self {
            return \(tryKeyword)\(awaitKeyword)Self(actorSystem: actorSystem)
          }
          """
      }
      generated.append(DeclSyntax(stringLiteral: body))
    }

    // No-dependency actors get a `None` to pass at the call site — unless
    // they already declared one themselves.
    let hasNone = members.contains { decl in
      if let typeAlias = decl.as(TypeAliasDeclSyntax.self) { return typeAlias.name.text == "None" }
      if let structDecl = decl.as(StructDeclSyntax.self) { return structDecl.name.text == "None" }
      return false
    }
    if !hasDependency && !hasNone {
      generated.append("public struct None: Codable, Sendable {}")
    }

    return generated
  }

  public static func expansion(
    of node: AttributeSyntax,
    attachedTo declaration: some DeclGroupSyntax,
    providingExtensionsOf type: some TypeSyntaxProtocol,
    conformingTo protocols: [TypeSyntax],
    in context: some MacroExpansionContext
  ) throws -> [ExtensionDeclSyntax] {
    let decl: DeclSyntax = "extension \(type.trimmed): VirtualActor {}"
    return [decl.cast(ExtensionDeclSyntax.self)]
  }
}

@main
struct VirtualActorsPlugin: CompilerPlugin {
  let providingMacros: [Macro.Type] = [
    VirtualActorMacro.self
  ]
}
