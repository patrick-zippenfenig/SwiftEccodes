import XCTest
@testable import SwiftEccodes

final class SwiftEccodesTests: XCTestCase {
    func testExample() throws {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(SwiftEccodes().text, "Hello, World!")
        
        let file = try GribFile(file: "/Users/patrick/Downloads/test.grib")
        for message in file.messages {
            message.iterate(namespace: "ls").forEach({
                print($0)
            })
            let data = message.getData()
            print(data[0..<10])
        }
    }
}
