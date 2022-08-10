@_implementationOnly import CEccodes

public enum EccodesError: Error {
    case cannotOpenFile(filename: String, errno: Int32, error: String)
    case cannotGetData
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

/// A GRIB file on disk
public final class GribFile {
    let fn: UnsafeMutablePointer<FILE>
    
    /// Iterate through all GRID messages
    public var messages: AnyIterator<GribMessage> {
        let c = grib_context_get_default()
        return AnyIterator<GribMessage> {
            var error: Int32 = 0
            codes_grib_multi_support_on(c)
            defer {
                codes_grib_multi_support_off(c)
            }
            guard let h = codes_handle_new_from_file(c, self.fn, PRODUCT_GRIB, &error) else {
                return nil
            }
            guard grib_is_defined(h, "7777") == 1 else {
                fatalError("Invalid grib message")
            }
            return GribMessage(h: h)
        }
    }
    
    /// Try to open file for reading. Throws an error if the file could not be opened
    public init(file: String) throws {
        guard let fn = fopen(file, "r") else {
            let error = String(cString: strerror(errno))
            throw EccodesError.cannotOpenFile(filename: file, errno: errno, error: error)
        }
        self.fn = fn
    }
}

/// A GRIB file in memory
public struct GribMemory {
    let ptr: UnsafeRawBufferPointer
    
    /// The pointer must be valid for the time it is used to read grib data
    public init(ptr: UnsafeRawBufferPointer) {
        self.ptr = ptr
    }
    
    /// Iterate through all GRID messages
    public var messages: AnyIterator<GribMessage> {
        guard let baseAddress = ptr.baseAddress else {
            fatalError()
        }
        let c = grib_context_get_default()
        
        var length = ptr.count
        var error: Int32 = 0
        var ptrs: [UnsafeMutableRawPointer?] = [UnsafeMutableRawPointer(mutating: baseAddress)]
        
        return AnyIterator<GribMessage> {
            codes_grib_multi_support_on(c)
            defer {
                codes_grib_multi_support_off(c)
            }
            guard let h = codes_grib_handle_new_from_multi_message(c, &ptrs, &length, &error) else {
                return nil
            }
            guard grib_is_defined(h, "7777") == 1 else {
                fatalError("Invalid grib message")
            }
            return GribMessage(h: h)
        }
    }
}

public final class GribMessage {
    let h: OpaquePointer
    
    public init(h: OpaquePointer) {
        self.h = h
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
