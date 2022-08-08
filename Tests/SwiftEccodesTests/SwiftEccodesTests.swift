import XCTest
import SwiftEccodes

final class SwiftEccodesTests: XCTestCase {
    func testExample() throws {
        let file = try GribFile(file: "/Users/patrick/Downloads/test.grib")
        for message in file.messages {
            message.iterate(namespace: .ls).forEach({
                print($0)
            })
            print(message.get(attribute: "name")!)
            let data = try message.getDouble()
            print(data[0..<10])
        }
    }
    
    func testExample2() throws {
        let data = try Data(contentsOf: URL(fileURLWithPath: "/Users/patrick/Downloads/test.grib"))
        try data.withUnsafeBytes { ptr in
            let file = GribMemory(ptr: ptr)
            for message in file.messages {
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
    }
}
