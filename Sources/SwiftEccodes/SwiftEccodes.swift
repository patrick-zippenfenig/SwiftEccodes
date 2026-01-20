import CEccodes
import Foundation


public enum EccodesError: Error {
    case cannotOpenFile(filename: String)
    case cannotGetData
    case invalidGribFileTrailingMessage7777IsMissing
    case newFromMultiMessageFailed(error: Int32)
    case newFromFileFailed(error: Int32)
}

public enum EccodesNamespace: String {
    case ls
    case parameter
    case statistics
    case time
    case geography
    case vertial
    case mars
    case all = ""
}


/// Contains function to read grib messages
public struct SwiftEccodes {
    /// Eccodes grib context might not be thread safe.
    fileprivate static let gribContextLock = Lock()
    
    /// Open a file and return all messages. Load the entire file into memory.
    public static func getMessages(fileName: String, multiSupport: Bool) throws -> [GribMessage] {
        guard let fileHandle = FileHandle(forReadingAtPath: fileName) else {
            throw EccodesError.cannotOpenFile(filename: fileName)
        }
        return try Self.getMessages(fileHandle: fileHandle, multiSupport: multiSupport)

    }
    
    /// Return all messages. Load the entire file into memory.
    public static func getMessages(fileHandle: FileHandle, multiSupport: Bool) throws -> [GribMessage] {
        let c = getContext(multiSupport: multiSupport)
        let fn = fdopen(dup(fileHandle.fileDescriptor), "r")
        defer { fclose(fn) }
        
        var messages = [GribMessage]()
        while true {
            var error: Int32 = 0
            guard let h = gribContextLock.withLock({ codes_handle_new_from_file(c, fn, PRODUCT_GRIB, &error) }) else {
                guard error == 0 else {
                    throw EccodesError.newFromFileFailed(error: error)
                }
                break
            }
            messages.append(try GribMessage(h: h))
        }
        return messages
    }
    
    /// Read all messages from memory. Memory must not be freed until all messages have been consumed.
    /// The message will be copied as soon as a modification is needed. In practice, memory copy is very likely.
    public static func getMessages(memory: UnsafeRawBufferPointer, multiSupport: Bool) throws -> [GribMessage] {
        let c = gribContextLock.withLock {
            let c = grib_context_get_default()
            if multiSupport {
                codes_grib_multi_support_on(c)
            } else {
                codes_grib_multi_support_off(c)
            }
            return c
        }
        
        var messages = [GribMessage]()
        var length = memory.count
        var ptrs = UnsafeMutableRawPointer(mutating: memory.baseAddress)
        while true {
            var error: Int32 = 0
            guard let h = gribContextLock.withLock({ codes_grib_handle_new_from_multi_message(c, &ptrs, &length, &error) }) else {
                guard error == 0 else {
                    throw EccodesError.newFromMultiMessageFailed(error: error)
                }
                break
            }
            messages.append(try GribMessage(h: h))
        }
        return messages
    }
    
    /// Open a file and iterate all messages in an AsyncSequence. The async sequence is used for error propagation. No async IO takes place.
    public static func iterateMessages(fileName: String, multiSupport: Bool) throws(EccodesError) -> GribFileAsyncSequence {
        return try GribFileAsyncSequence(fileName: fileName, multiSupport: multiSupport)
    }
    
    /// Open a file and iterate all messages in an AsyncSequence. The async sequence is used for error propagation.
    public static func iterateMessages(memory: UnsafeRawBufferPointer, multiSupport: Bool) throws(EccodesError) -> GribMemoryAsyncSequence {
        return GribMemoryAsyncSequence(memory: memory, multiSupport: multiSupport)
    }
    
    /// Detect a range of bytes in a byte stream if there is a grib header and returns it
    /// Note: The required length to decode a GRIB message is not checked of the input buffer
    public static func seekGrib(memory: UnsafeRawBufferPointer) -> (offset: Int, length: Int)? {
        let search = "GRIB"
        guard let base = memory.baseAddress else {
            return nil
        }
        guard let offset = search.withCString({memory.firstRange(of: UnsafeRawBufferPointer(start: $0, count: strlen($0)))})?.lowerBound else {
            return nil
        }
        guard offset <= (1 << 40),
              offset + MemoryLayout<GribHeader>.size <= memory.count else {
            return nil
        }
        struct GribHeader {
            /// "GRIB"
            let magic: UInt32
            
            /// Should be zero
            let zero: UInt16
            
            /// 0 - for Meteorological Products, 2 for Land Surface Products, 10 - for Oceanographic Products
            let type: UInt8
            
            /// Version 1 and 2 supported
            let version: UInt8
            
            /// Endian needs to be swapped
            let length: UInt64
        }
        
        let header = base.advanced(by: offset).assumingMemoryBound(to: GribHeader.self)
        let length = header.pointee.length.bigEndian
        guard header.pointee.zero == 0,
              (1...2).contains(header.pointee.version),
              length <= (1 << 40) else {
            return nil
        }
        return (offset, Int(length))
    }
    
    static func getContext(multiSupport: Bool) -> OpaquePointer? {
        return gribContextLock.withLock {
            let c = grib_context_get_default()
            if multiSupport {
                codes_grib_multi_support_on(c)
            } else {
                codes_grib_multi_support_off(c)
            }
            return c
        }
    }
}

/// Async sequence to iterate over GRIB messages in a file
public struct GribFileAsyncSequence: AsyncSequence {
    let multiSupport: Bool
    let fileHandle: FileHandle
    
    public init(fileName: String, multiSupport: Bool) throws(EccodesError) {
        guard let fh = FileHandle(forReadingAtPath: fileName) else {
            throw EccodesError.cannotOpenFile(filename: fileName)
        }
        self.fileHandle = fh
        self.multiSupport = multiSupport
    }
    
    public class AsyncIterator: AsyncIteratorProtocol {
        let context: OpaquePointer?
        var fn: UnsafeMutablePointer<FILE>? = nil

        init(fileHandle: FileHandle, multiSupport: Bool) {
            /// Duplicate file descriptor, because `fclose` would close the fileHandle leading to double free
            self.fn = fdopen(dup(fileHandle.fileDescriptor), "r")
            context = SwiftEccodes.getContext(multiSupport: multiSupport)
        }

        public func next() async throws(EccodesError) -> GribMessage? {
            guard let fn else {
                return nil
            }
            var error: Int32 = 0
            guard let h = SwiftEccodes.gribContextLock.withLock({ codes_handle_new_from_file(context, fn, PRODUCT_GRIB, &error) }) else {
                guard error == 0 else {
                    throw EccodesError.newFromFileFailed(error: error)
                }
                fclose(fn)
                self.fn = nil
                return nil
            }
            return try GribMessage(h: h)
        }
        
        deinit {
            if let fn {
                fclose(fn)
                self.fn = nil
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return AsyncIterator(fileHandle: fileHandle, multiSupport: multiSupport)
    }
}


/// Async sequence to iterate over GRIB messages in a memory region
public struct GribMemoryAsyncSequence: AsyncSequence, @unchecked Sendable {
    let memory: UnsafeRawBufferPointer
    let multiSupport: Bool
    
    public init(memory: UnsafeRawBufferPointer, multiSupport: Bool){
        self.memory = memory
        self.multiSupport = multiSupport
    }
    
    public struct AsyncIterator: AsyncIteratorProtocol {
        let context: OpaquePointer?
        var length: Int
        var memory: UnsafeMutableRawPointer?

        init(memory: UnsafeRawBufferPointer, multiSupport: Bool) {
            self.memory = UnsafeMutableRawPointer(mutating: memory.baseAddress)
            self.length = memory.count
            context = SwiftEccodes.getContext(multiSupport: multiSupport)
        }

        mutating public func next() async throws(EccodesError) -> GribMessage? {
            guard memory != nil else {
                return nil
            }
            var error: Int32 = 0
            guard let h = SwiftEccodes.gribContextLock.withLock({ codes_grib_handle_new_from_multi_message(context, &memory, &length, &error) }) else {
                guard error == 0 else {
                    throw EccodesError.newFromMultiMessageFailed(error: error)
                }
                memory = nil
                return nil
            }
            return try GribMessage(h: h)
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return AsyncIterator(memory: memory, multiSupport: multiSupport)
    }
}

/// Represent a GRIB message. Frees memory at release.
public final class GribMessage {
    let h: OpaquePointer
    
    init(h: OpaquePointer) throws(EccodesError) {
        guard grib_is_defined(h, "7777") == 1 else {
            codes_handle_delete(h)
            throw EccodesError.invalidGribFileTrailingMessage7777IsMissing
        }
        
        self.h = h
    }
    
    /// Iterate over all grid-cells and return coordinate and value
    public func iterateCoordinatesAndValues() throws -> AnyIterator<(latitude: Double, longitude: Double, value: Double)> {
        var error: Int32 = 0
        let iterator = codes_grib_iterator_new(h, 0, &error)
        guard error == CODES_SUCCESS else {
            fatalError("codes_grib_iterator_new failed")
        }
        let bitmap = try getBitmap()
        var lat: Double = .nan
        var lon: Double = .nan
        var value: Double = .nan
        var i = 0
        return AnyIterator {
            guard (codes_grib_iterator_next(iterator, &lat, &lon, &value) != 0) else {
                codes_grib_iterator_delete(iterator)
                return nil
            }
            if let bitmap = bitmap, bitmap[i] == 0 {
                value = .nan
            }
            i += 1
            return (lat,lon,value)
        }
    }
    
    /// Load Bitmap into an existing array. If the bitmap value is 0. NaN should be assumed for this location
    public func loadBitmap(into: inout [Int]) throws -> Bool {
        guard let bitmapPresent = getLong(attribute: "bitmapPresent"), bitmapPresent == 1 else {
            return false
        }
        var size = try getSize(of: "bitmap")
        // shrink if required
        let _ = into.dropLast(max(0, into.count - size))
        // grow if required
        for _ in 0..<size-into.count {
            into.append(0)
        }
        try into.withUnsafeMutableBufferPointer { buffer in
            guard codes_get_long_array(h, "bitmap", buffer.baseAddress, &size) == 0 else {
                throw EccodesError.cannotGetData
            }
        }
        return true
    }
    
    /// Load values into an existing double array. NaN are  not checked from the bitmap
    public func loadDoubleNotNaNChecked(into: inout [Double]) throws {
        var size = try getSize(of: "values")
        
        // shrink if required
        let _ = into.dropLast(max(0, into.count - size))
        // grow if required
        for _ in 0..<size-into.count {
            into.append(0)
        }
        try into.withUnsafeMutableBufferPointer { buffer in
            guard codes_get_double_array(h, "values", buffer.baseAddress, &size) == 0 else {
                throw EccodesError.cannotGetData
            }
        }
    }
    
    /// Read data as `Double` array
    public func getDouble() throws -> [Double] {
        var size = try getSize(of: "values")
        
        var data = try [Double](unsafeUninitializedCapacity: size) { buffer, initializedCount in
            guard codes_get_double_array(h, "values", buffer.baseAddress, &size) == 0 else {
                throw EccodesError.cannotGetData
            }
            initializedCount += size
        }
        
        /// Filter NaNs
        guard let bitmap = try getBitmap() else {
            return data
        }
        for i in data.indices {
            if bitmap[i] == 0 {
                data[i] = .nan
            }
        }
        return data
    }
    
    /// Get bitmap for information if a value is set
    public func getBitmap() throws -> [Int]? {
        guard let bitmapPresent = getLong(attribute: "bitmapPresent"), bitmapPresent == 1 else {
            return nil
        }
        var size = try getSize(of: "bitmap")
        return try [Int](unsafeUninitializedCapacity: size) { buffer, initializedCount in
            guard codes_get_long_array(h, "bitmap", buffer.baseAddress, &size) == 0 else {
                throw EccodesError.cannotGetData
            }
            initializedCount += size
        }
    }
    
    /// Read data as `Int` array
    public func getLong() throws -> [Int] {
        var size = try getSize(of: "values")
        
        return try [Int](unsafeUninitializedCapacity: size) { buffer, initializedCount in
            guard codes_get_long_array(h, "values", buffer.baseAddress, &size) == 0 else {
                throw EccodesError.cannotGetData
            }
            initializedCount += size
        }
    }
    
    /// Get a number of elements of an encoded array.
    public func getSize(of key: String) throws -> Int {
        var size = 0
        guard codes_get_size(h, key, &size) == 0 else {
            throw EccodesError.cannotGetData
        }
        return size
    }
    
    /// Get a single attribute. E.g. `name`, `Ni` or `Nj`
    public func get(attribute: String) -> String? {
        var length = 0
        guard grib_get_length(h, attribute, &length) == 0 else {
            return nil
        }
        return String(unsafeUninitializedCapacity: length) { buffer in
            guard codes_get_string(h, attribute, buffer.baseAddress, &length) == 0 else {
                fatalError()
            }
            if buffer[length-1] == 0 {
                // ignore zero terminator
                return length-1
            }
            return length
        }
    }
    
    /// Get a single attribute as integer, E.g. `message.getLong(attribute: "parameterNumber")` return 192
    public func getLong(attribute: String) -> Int? {
        var value = Int(0)
        guard codes_get_long(h, attribute, &value) == 0 else {
            return nil
        }
        return value
    }
    
    /// Itterate through all attributes in a given namespace as key value string tuples
    public func iterate(namespace: EccodesNamespace) -> AnyIterator<(key: String, value: String)> {
        guard let kiter  = codes_keys_iterator_new(h, UInt(CODES_KEYS_ITERATOR_ALL_KEYS | CODES_KEYS_ITERATOR_SKIP_DUPLICATES), namespace.rawValue) else {
            fatalError()
        }

        return AnyIterator {
            guard codes_keys_iterator_next(kiter) == 1 else {
                codes_keys_iterator_delete(kiter)
                return nil
            }
            guard let attribute = codes_keys_iterator_get_name(kiter) else {
                return nil
            }
            let key = String(cString: attribute)
            var length = 0
            guard grib_get_length(self.h, attribute, &length) == 0 else {
                return nil
            }
            guard length > 0, key != "bitmap" else {
                return (key, "")
            }
            let value = String(unsafeUninitializedCapacity: length) { buffer in
                guard codes_keys_iterator_get_string(kiter, buffer.baseAddress, &length) == 0 else {
                    fatalError()
                }
                if buffer[length-1] == 0 {
                    // ignore zero terminator
                    return length-1
                }
                return length
            }
            return (key, value)
        }
    }
    
    deinit {
        codes_handle_delete(h)
    }
}
