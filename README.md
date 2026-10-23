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

Decoding calls may run concurrently. SwiftEccodes protects context configuration
and the complete memory decode with one lock, including pending fields and
error cleanup. File reads configure their mode under the same lock.
This coordination covers SwiftEccodes APIs; direct calls to the
ecCodes C API must not change its default context concurrently.

The memory async sequence decodes and buffers its input on the first `next()` call,
then yields fields in input order. It buffers the entire supplied region, increasing
first-result latency and memory use compared with decoding one field per call,
but allows iterators to be interleaved or abandoned without leaving shared pending
fields. The file async sequence continues to decode one field per call.
No thread lock is held across `await`. A memory decoding error is thrown after
any successfully decoded prefix, and subsequent `next()` calls return `nil`.
Malformed GRIB2 framing is rejected before any fields from that physical message
are decoded. The existing file-iterator cleanup behavior is unchanged.

For memory APIs, keep the input buffer alive and unchanged until the sequence and
all returned messages have been consumed. Buffering does not relax this lifetime
requirement. File APIs manage their input storage internally.
