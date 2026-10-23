import CEccodes
import Foundation
import XCTest
import SwiftEccodes

final class MultiFieldTests: XCTestCase {
    // Generate actual multi-field GRIB2 messages before starting concurrent work.
    private func fixture(_ values: [Double]) throws -> Data {
        // Loading samples with multi support enabled itself creates FILE reader
        // state. Keep fixture generation out of the state being tested.
        codes_grib_multi_support_off(nil)
        let sample = try XCTUnwrap(codes_grib_handle_new_from_samples(nil, "regular_ll_sfc_grib2"))
        defer { codes_handle_delete(sample) }
        let multi = try XCTUnwrap(codes_grib_multi_handle_new(nil))
        defer { codes_grib_multi_handle_delete(multi) }
        for value in values {
            let h = try XCTUnwrap(codes_handle_clone(sample))
            defer { codes_handle_delete(h) }
            XCTAssertEqual(codes_set_long(h, "Ni", 2), 0)
            XCTAssertEqual(codes_set_long(h, "Nj", 2), 0)
            let grid = [value, value + 1, value + 2, value + 3]
            XCTAssertEqual(grid.withUnsafeBufferPointer { codes_set_double_array(h, "values", $0.baseAddress, $0.count) }, 0)
            XCTAssertEqual(codes_grib_multi_handle_append(h, 4, multi), 0)
        }
        let file = try XCTUnwrap(tmpfile())
        defer { fclose(file) }
        XCTAssertEqual(codes_grib_multi_handle_write(multi, file), 0)
        let length = ftell(file)
        rewind(file)
        var data = Data(count: length)
        XCTAssertEqual(data.withUnsafeMutableBytes { fread($0.baseAddress, 1, length, file) }, length)
        return data
    }

    private func expected(_ values: [Double]) -> [[Double]] {
        values.map { [$0, $0 + 1, $0 + 2, $0 + 3] }
    }

    private func decode(_ data: Data, multi: Bool = true) throws -> [[Double]] {
        try data.withUnsafeBytes {
            try SwiftEccodes.getMessages(memory: $0, multiSupport: multi).map { try $0.getDouble() }
        }
    }

    func testOrderAndConcatenation() throws {
        let a = try fixture([10, 20, 30])
        let b = try fixture([40])
        XCTAssertEqual(try decode(a), expected([10, 20, 30]))
        XCTAssertEqual(try decode(a + b + a), expected([10, 20, 30, 40, 10, 20, 30]))
        XCTAssertEqual(try decode(b), expected([40]))
        XCTAssertEqual(try decode(b, multi: false), expected([40]))
    }

    func testGRIB1Compatibility() throws {
        let h = try XCTUnwrap(codes_grib_handle_new_from_samples(nil, "regular_ll_sfc_grib1"))
        defer { codes_handle_delete(h) }
        XCTAssertEqual(codes_set_long(h, "Ni", 2), 0)
        XCTAssertEqual(codes_set_long(h, "Nj", 2), 0)
        let values = [1.0, 2, 3, 4]
        XCTAssertEqual(values.withUnsafeBufferPointer { codes_set_double_array(h, "values", $0.baseAddress, $0.count) }, 0)
        var pointer: UnsafeRawPointer?
        var size = 0
        XCTAssertEqual(codes_get_message(h, &pointer, &size), 0)
        let data = Data(bytes: try XCTUnwrap(pointer), count: size)
        let multi = try fixture([10, 20])
        for enabled in [true, false] {
            XCTAssertEqual(try decode(data, multi: enabled), [values])
            let wanted = enabled ? expected([1, 10, 20, 1]) : expected([1, 10, 1])
            XCTAssertEqual(try decode(data + multi + data, multi: enabled), wanted)
        }
    }

    func testConcurrentMemoryDecodes() throws {
        let inputs = try (0..<16).map { i in try fixture((0..<8).map { Double(i * 100 + $0 * 10) }) }
        // Only failure reporting is locked; decodes are deliberately concurrent.
        let failuresLock = NSLock()
        var failures = [String]()
        DispatchQueue.concurrentPerform(iterations: 128) { iteration in
            let stream = iteration % inputs.count
            do {
                let actual = try decode(inputs[stream])
                let wanted = expected((0..<8).map { Double(stream * 100 + $0 * 10) })
                if actual != wanted {
                    failuresLock.lock()
                    failures.append("stream \(stream): \(actual.map { $0.first ?? -1 })")
                    failuresLock.unlock()
                }
            } catch {
                failuresLock.lock()
                failures.append("stream \(stream): \(error)")
                failuresLock.unlock()
            }
        }
        XCTAssertTrue(failures.isEmpty, "\(failures.count) failed decodes: \(failures.prefix(5))")
    }

    // Stable storage survives suspension and all yielded handles.
    private final class Buffer: @unchecked Sendable {
        let bytes: UnsafeMutableRawBufferPointer
        init(_ data: Data) {
            bytes = .allocate(byteCount: data.count, alignment: 8)
            _ = data.copyBytes(to: bytes)
        }
        deinit { bytes.deallocate() }
        var sequence: GribMemoryAsyncSequence {
            GribMemoryAsyncSequence(memory: UnsafeRawBufferPointer(bytes), multiSupport: true)
        }
    }

    func testInterleavedMemoryIterators() async throws {
        let a = Buffer(try fixture([10, 20, 30]))
        let b = Buffer(try fixture([100, 200, 300]))
        var first = a.sequence.makeAsyncIterator()
        var second = b.sequence.makeAsyncIterator()
        for (x, y) in zip([10.0, 20, 30], [100.0, 200, 300]) {
            let left = try await first.next()
            await Task.yield()
            let right = try await second.next()
            XCTAssertEqual(try left?.getDouble(), expected([x]).first)
            XCTAssertEqual(try right?.getDouble(), expected([y]).first)
        }
        let endA = try await first.next()
        let endB = try await second.next()
        XCTAssertNil(endA)
        XCTAssertNil(endB)
    }

    func testAsyncConcatenationAndDisabledMultiSupport() async throws {
        let buffer = Buffer(try fixture([10, 20, 30]) + fixture([40]))
        for enabled in [true, false] {
            let sequence = GribMemoryAsyncSequence(memory: UnsafeRawBufferPointer(buffer.bytes), multiSupport: enabled)
            var actual = [[Double]]()
            for try await message in sequence {
                actual.append(try message.getDouble())
                await Task.yield()
            }
            XCTAssertEqual(actual, expected(enabled ? [10, 20, 30, 40] : [10, 40]))
        }
        let empty = Buffer(Data())
        var iterator = empty.sequence.makeAsyncIterator()
        let end = try await iterator.next()
        XCTAssertNil(end)
    }

    func testConcurrentAsyncAndEarlyTermination() async throws {
        let buffers = try (0..<16).map { i -> Buffer in
            let base = Double(i * 100)
            return Buffer(try fixture([base, base + 10, base + 20]))
        }
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<64 {
                group.addTask {
                    let stream = i % buffers.count
                    var iterator = buffers[stream].sequence.makeAsyncIterator()
                    var actual = [[Double]]()
                    while let message = try await iterator.next() {
                        actual.append(try message.getDouble())
                        await Task.yield()
                        if i % 2 == 0 { break }
                    }
                    let wanted = self.expected((0..<(i % 2 == 0 ? 1 : 3)).map { Double(stream * 100 + $0 * 10) })
                    XCTAssertEqual(actual, wanted)
                }
            }
            try await group.waitForAll()
        }
        XCTAssertEqual(try decode(fixture([700, 800])), expected([700, 800]))
    }

    func testMalformedRecovery() async throws {
        let good = try fixture([10, 20, 30])
        var missingEnd = good
        missingEnd.replaceSubrange((missingEnd.count - 4)..<missingEnd.count, with: [0, 0, 0, 0])
        var invalidSection = good
        invalidSection[20] = 9
        var unsupported = good
        unsupported[7] = 3
        var shortSection = good
        shortSection.replaceSubrange(16..<20, with: [0, 0, 0, 1])
        var invalidLaterField = good
        var section = 16
        while section < good.count - 4 {
            let size = good[section..<section + 4].reduce(0) { ($0 << 8) | Int($1) }
            let kind = good[section + 4]
            section += size
            if kind == 7 {
                invalidLaterField[section + 4] = 9
                break
            }
        }
        for bad in [Data(good.dropLast(2)), missingEnd, invalidSection, unsupported, shortSection, invalidLaterField] {
            XCTAssertThrowsError(try decode(bad))
            XCTAssertEqual(try decode(good), expected([10, 20, 30]))
            // Non-multi mode retains ecCodes' own acceptance/error behavior.
            _ = try? decode(bad, multi: false)
            XCTAssertEqual(try decode(good), expected([10, 20, 30]))
            let buffer = Buffer(good + bad)
            var iterator = buffer.sequence.makeAsyncIterator()
            for value in [10.0, 20, 30] {
                let message = try await iterator.next()
                XCTAssertEqual(try message?.getDouble(), expected([value]).first)
            }
            do {
                _ = try await iterator.next()
                XCTFail("Malformed input must throw")
            } catch { }
            let end = try await iterator.next()
            XCTAssertNil(end)
            XCTAssertEqual(try decode(good), expected([10, 20, 30]))
        }
        XCTAssertEqual(try decode(Data()), [])
    }

    func testMixedMemoryAndFileModes() async throws {
        let data = try fixture([10, 20, 30]) + fixture([40])
        let single = try fixture([900])
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try data.write(to: url)
        defer { try? FileManager.default.removeItem(at: url) }
        let buffer = Buffer(data)
        // Constructing an iterator must not configure the shared context.
        var memory = buffer.sequence.makeAsyncIterator()
        let file = try SwiftEccodes.iterateMessages(fileName: url.path, multiSupport: true).makeAsyncIterator()
        let firstFile = try await file.next()
        XCTAssertEqual(try firstFile?.getDouble(), expected([10]).first)
        XCTAssertEqual(try decode(single, multi: false), expected([900]))
        let firstMemory = try await memory.next()
        XCTAssertEqual(try firstMemory?.getDouble(), expected([10]).first)
        for value in [20.0, 30, 40] {
            XCTAssertEqual(try decode(single, multi: false), expected([900]))
            let nextFile = try await file.next()
            let nextMemory = try await memory.next()
            XCTAssertEqual(try nextFile?.getDouble(), expected([value]).first)
            XCTAssertEqual(try nextMemory?.getDouble(), expected([value]).first)
        }
        let wanted = expected([10, 20, 30, 40])
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<48 {
                group.addTask {
                    switch i % 4 {
                    case 0:
                        let messages = try SwiftEccodes.getMessages(fileName: url.path, multiSupport: true)
                        XCTAssertEqual(try messages.map { try $0.getDouble() }, wanted)
                    case 1:
                        XCTAssertEqual(try self.decode(single, multi: false), self.expected([900]))
                    case 2:
                        var iterator = buffer.sequence.makeAsyncIterator()
                        var values = [[Double]]()
                        while let message = try await iterator.next() {
                            values.append(try message.getDouble())
                            await Task.yield()
                        }
                        XCTAssertEqual(values, wanted)
                    default:
                        let sequence = try SwiftEccodes.iterateMessages(fileName: url.path, multiSupport: false)
                        var values = [[Double]]()
                        for try await message in sequence {
                            values.append(try message.getDouble())
                            await Task.yield()
                        }
                        XCTAssertEqual(values, self.expected([10, 40]))
                    }
                }
            }
            try await group.waitForAll()
        }
    }

}
