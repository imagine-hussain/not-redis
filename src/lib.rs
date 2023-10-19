use dashmap::DashMap;

/// A thread-safe, timed key-value store that allows expiration of keys.
///
struct TMap<K, V> {
    map: DashMap<K, V>,
}
