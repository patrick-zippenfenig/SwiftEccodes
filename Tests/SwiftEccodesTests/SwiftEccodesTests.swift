import XCTest
import SwiftEccodes

final class SwiftEccodesTests: XCTestCase {
    override func setUp() {
        let projectHome = String(#file[...#file.range(of: "/Tests/")!.lowerBound])
        FileManager.default.changeCurrentDirectoryPath(projectHome)
    }
    
    func testExample() throws {
        let file = try GribFile(file: "Tests/test.grib")
        XCTAssertEqual(file.messages.count, 2)
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
        // Multi part grib files are the result of using range downloads via CURL
        let data = try Data(contentsOf: URL(fileURLWithPath: "Tests/multipart.grib"))
        try data.withUnsafeBytes { ptr in
            let file = try GribMemory(ptr: ptr)
            XCTAssertEqual(file.messages.count, 2)
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
    
    func testNans() throws {
        let file = try GribFile(file: "Tests/soil_moisture_with_nans.grib")
        XCTAssertEqual(file.messages.count, 1)
        let message = file.messages[0]
        let data = try message.getDouble()
        XCTAssertEqual(data.count, 72960)
        XCTAssertTrue(data[0].isNaN)
        XCTAssertFalse(data[2984].isNaN)
    }
    
    func testIterateCoordinates() throws {
        let file = try GribFile(file: "Tests/test.grib")
        let message = file.messages[0]
        let lons = try message.iterateCoordinatesAndValues().map { $0.longitude }
        let lats = try message.iterateCoordinatesAndValues().map { $0.latitude }
        XCTAssertEqual(lons[0..<10], [0.0, 0.9374986945169713, 1.8749973890339426, 2.812496083550914, 3.7499947780678853, 4.6874934725848565, 5.624992167101828, 6.5624908616188, 7.4999895561357715, 8.437488250652743])
        XCTAssertEqual(lats[0..<10], [89.27671287810583, 89.27671287810583, 89.27671287810583, 89.27671287810583, 89.27671287810583, 89.27671287810583, 89.27671287810583, 89.27671287810583, 89.27671287810583, 89.27671287810583])
    }
}
