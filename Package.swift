// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let platforms: [SupportedPlatform]?
#if os(Linux)
platforms = nil
#else
platforms = [
  // we require the 'distributed actor' language and runtime feature:
  .iOS(.v18),
  .macOS(.v15),
  .tvOS(.v18),
  .watchOS(.v11),
]
#endif

let package = Package(
  name: "cluster-virtual-actors",
  platforms: platforms,
  products: [
    // Products define the executables and libraries a package produces, making them visible to other packages.
    .library(
      name: "VirtualActors",
      targets: ["VirtualActors"]
    )
  ],
  dependencies: [
    // TODO: Update when all PRs are merged
    .package(
      url: "https://github.com/akbashev/swift-distributed-actors.git",
      branch: "presentation"
    ),
    .package(url: "https://github.com/apple/swift-async-algorithms.git", from: "1.0.0"),
  ],
  targets: [
    // Targets are the basic building blocks of a package, defining a module or a test suite.
    // Targets can depend on other targets in this package and products from dependencies.
    .target(
      name: "VirtualActors",
      dependencies: [
        .product(
          name: "DistributedCluster",
          package: "swift-distributed-actors"
        ),
        .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
      ]
    ),
    .testTarget(
      name: "VirtualActorsTests",
      dependencies: [
        "VirtualActors"
      ]
    ),
  ]
)
