import CEccodes

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
}

public final class GribFile {
    let fn: UnsafeMutablePointer<FILE>
    
    public var messages: AnyIterator<GribMessage> {
        AnyIterator<GribMessage> {
            var error: Int32 = 0
            guard let h = codes_handle_new_from_file(nil, self.fn, PRODUCT_GRIB, &error) else {
                return nil
            }
            return GribMessage(h: h)
        }
    }
    
    public init(file: String) throws {
        guard let fn = fopen(file, "r") else {
            let error = String(cString: strerror(errno))
            throw EccodesError.cannotOpenFile(filename: file, errno: errno, error: error)
        }
        self.fn = fn
    }
}

public struct GribMemory {
    let ptr: UnsafeRawBufferPointer
    
    public init(ptr: UnsafeRawBufferPointer) {
        self.ptr = ptr
    }
    
    public var messages: AnyIterator<GribMessage> {
        var offset = 0
        return AnyIterator<GribMessage> {
            if offset >= ptr.count {
                return nil
            }
            guard let h = codes_handle_new_from_message(nil, ptr.baseAddress?.advanced(by: offset), ptr.count) else {
                return nil
            }
            var size = 0
            codes_get_message_size(h, &size)
            offset += size
            return GribMessage(h: h)
        }
    }
}

public final class GribMessage {
    let h: OpaquePointer
    
    public init(h: OpaquePointer) {
        self.h = h
    }
    
    public func getData() throws -> [Double] {
        var size = 0
        guard codes_get_size(h, "values", &size) == 0 else {
            throw EccodesError.cannotGetData
        }
        
        return try [Double](unsafeUninitializedCapacity: size) { buffer, initializedCount in
            guard codes_get_double_array(h, "values", buffer.baseAddress, &size) == 0 else {
                throw EccodesError.cannotGetData
            }
            initializedCount += size
        }
    }
    
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
