@_implementationOnly import CEccodes

import Foundation
#if os(Linux)
    import Glibc
#else
    import Darwin
#endif


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
    /// Open a file and return all messages. Load the entire file into memory.
    public static func getMessages(fileName: String, multiSupport: Bool) throws -> [GribMessage] {
        var messages = [GribMessage]()
        try iterateMessages(fileName: fileName, multiSupport: multiSupport) {
            messages.append($0)
        }
        return messages
    }
    
    /// Return all messages. Load the entire file into memory.
    public static func getMessages(fileHandle: FileHandle, multiSupport: Bool) throws -> [GribMessage] {
        var messages = [GribMessage]()
        try iterateMessages(fileHandle: fileHandle, multiSupport: multiSupport) {
            messages.append($0)
        }
        return messages
    }
    
    /// Read all messages from memory. Memory must not be freed until all messages have been consumed.
    /// The message will be copied as soon as a modification is needed. In practice, memory copy is very likely.
    public static func getMessages(memory: UnsafeRawBufferPointer, multiSupport: Bool) throws -> [GribMessage] {
        var messages = [GribMessage]()
        try iterateMessages(memory: memory, multiSupport: multiSupport) {
            messages.append($0)
        }
        return messages
    }
    
    /// Open a file and iterate all messages. All messages copy the required data into memory and the file does not need to stay open.
    public static func iterateMessages(fileName: String, multiSupport: Bool, callback: (GribMessage) throws -> ()) throws {
        guard let filehandle = FileHandle(forReadingAtPath: fileName) else {
            throw EccodesError.cannotOpenFile(filename: fileName)
        }
        try iterateMessages(fileHandle: filehandle, multiSupport: multiSupport, callback: callback)
    }
    
    /// Itterate all message from a given `FileHandle`. All messages copy the required data into memory and the file does not need to stay open.
    public static func iterateMessages(fileHandle: FileHandle, multiSupport: Bool, callback: (GribMessage) throws -> ()) throws {
        let c = grib_context_get_default()
        if multiSupport {
            codes_grib_multi_support_on(c)
        } else {
            codes_grib_multi_support_off(c)
        }
        
        let fn = fdopen(fileHandle.fileDescriptor, "r")
        
        while true {
            var error: Int32 = 0
            guard let h = codes_handle_new_from_file(c, fn, PRODUCT_GRIB, &error) else {
                guard error == 0 else {
                    throw EccodesError.newFromFileFailed(error: error)
                }
                return
            }
            try callback(try GribMessage(h: h))
        }
    }
    
    /// Iterate message from memory. Memory must not be freed until all messages have been consumed.
    /// The message will be copied as soon as a modification is needed. In practice, memory copy is very likely.
    public static func iterateMessages(memory: UnsafeRawBufferPointer, multiSupport: Bool = true, callback: (GribMessage) throws -> ()) throws {
        let c = grib_context_get_default()
        if multiSupport {
            codes_grib_multi_support_on(c)
        } else {
            codes_grib_multi_support_off(c)
        }
        
        var length = memory.count
        var ptrs = UnsafeMutableRawPointer(mutating: memory.baseAddress)
        while true {
            var error: Int32 = 0
            guard let h = codes_grib_handle_new_from_multi_message(c, &ptrs, &length, &error) else {
                guard error == 0 else {
                    throw EccodesError.newFromMultiMessageFailed(error: error)
                }
                return
            }
            try callback(try GribMessage(h: h))
        }
    }
    
    /// Detect a range of bytes in a byte stream if there is a grib header and returns it
    /// Note: The required length to decode a GRIB message is not checked of the input buffer
    public static func seekGrib(memory: UnsafeRawBufferPointer) -> (offset: Int, length: Int)? {
        let search = "GRIB"
        guard let base = memory.baseAddress else {
            return nil
        }
        guard let start = memmem(base, memory.count, search, search.count) else {
            return nil
        }
        let offset = base.distance(to: start)
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
        
        let header = start.assumingMemoryBound(to: GribHeader.self)
        let length = header.pointee.length.bigEndian
        guard header.pointee.zero == 0,
              (1...2).contains(header.pointee.version),
              length <= (1 << 40) else {
            return nil
        }
        return (offset, Int(length))
    }
}

/// Represent a GRIB message. Frees memory at release.
public final class GribMessage {
    let h: OpaquePointer
    
    init(h: OpaquePointer) throws {
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
        var bitmapPresent = 0
        guard codes_get_long(h, "bitmapPresent", &bitmapPresent) == 0, bitmapPresent == 1 else {
            return false
        }
        var size = 0
        guard codes_get_size(h, "bitmap", &size) == 0 else {
            fatalError("Could not get bitmap length")
        }
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
        var size = 0
        guard codes_get_size(h, "values", &size) == 0 else {
            throw EccodesError.cannotGetData
        }
        
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
        var size = 0
        guard codes_get_size(h, "values", &size) == 0 else {
            throw EccodesError.cannotGetData
        }
        
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
        var bitmapPresent = 0
        guard codes_get_long(h, "bitmapPresent", &bitmapPresent) == 0, bitmapPresent == 1 else {
            return nil
        }
        var size = 0
        guard codes_get_size(h, "bitmap", &size) == 0 else {
            fatalError("Could not get bitmap length")
        }
        return try [Int](unsafeUninitializedCapacity: size) { buffer, initializedCount in
            guard codes_get_long_array(h, "bitmap", buffer.baseAddress, &size) == 0 else {
                throw EccodesError.cannotGetData
            }
            initializedCount += size
        }
    }
    
    /// Read data as `Int` array
    public func getLong() throws -> [Int] {
        var size = 0
        guard codes_get_size(h, "values", &size) == 0 else {
            throw EccodesError.cannotGetData
        }
        
        return try [Int](unsafeUninitializedCapacity: size) { buffer, initializedCount in
            guard codes_get_long_array(h, "values", buffer.baseAddress, &size) == 0 else {
                throw EccodesError.cannotGetData
            }
            initializedCount += size
        }
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
            // ignore \0
            return length-1
        }
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
                // ignore \0
                return length-1
            }
            return (key, value)
        }
    }
    
    deinit {
        codes_handle_delete(h)
    }
}
