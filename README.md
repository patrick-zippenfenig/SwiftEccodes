# SwiftEccodes

A *very* basic library to read GRIB files in Swift. This library is a wrapper for [eccodes](https://github.com/ecmwf/eccodes).


## Usage
Install eccodes via `brew install eccodes` or `apt install libeccodes-dev`

Add `SwiftEccodes` as a dependency to your `Package.swift`

```swift
  dependencies: [
    .package(url: "https://github.com/patrick-zippenfenig/SwiftEccodes.git", from: "1.0.0")
  ],
  targets: [
    .target(name: "MyApp", dependencies: [
      .product(name: "SwiftEccodes", package: "SwiftEccodes"),
    ])
  ]
```

Read GRIB messages and data from files:

```swift
import SwiftEccodes

let messages = try SwiftEccodes.getMessages(fileName: "test.grib", multiSupport: true)
for message in messages {
    message.iterate(namespace: .ls).forEach({
        print($0)
    })
    print(message.get(attribute: "name")!)
    let data = try message.getDouble()
    print(data[0..<10])
}
```

Or read directly from memory:

```swift
import SwiftEccodes

let data = try Data(contentsOf: URL(fileURLWithPath: "test.grib"))
try data.withUnsafeBytes { ptr in
    let messages = try SwiftEccodes.getMessages(memory: ptr, multiSupport: true)
    for message in messages {
        message.iterate(namespace: .ls).forEach({
            print($0)
        })
        message.iterate(namespace: .geography).forEach({
            print($0)
        })
        print(message.get(attribute: "name")!)
        let data = try message.getDouble()
        print(data.count)
        print(data[0..<10])
    }
}
```

