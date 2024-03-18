use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use mwmr::{BTreeCm, Tm};

use super::{Options, PendingMap, InnerDB, ReadTransaction};

struct Inner<K, V> {
  tm: Tm<K, V, BTreeCm<K>, PendingMap<K, V>>,
  map: InnerDB<K, V>,
  max_batch_size: u64,
  max_batch_entries: u64,
}

impl<K, V> Inner<K, V> {
  fn new(name: &str, max_batch_size: u64, max_batch_entries: u64) -> Self {
    let map = SkipMap::new();
    let tm = Tm::new(name, 0);
    Self {
      tm,
      map: InnerDB(map),
      max_batch_size,
      max_batch_entries,
    }
  }
}

/// A concurrent ACID, MVCC in-memory database based on [`crossbeam-skiplist`][crossbeam_skiplist].
pub struct ComparableDB<K, V> {
  inner: Arc<Inner<K, V>>,
}

impl<K, V> Clone for ComparableDB<K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V> Default for ComparableDB<K, V> {
  /// Creates a new `ComparableDB` with the default options.
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> ComparableDB<K, V> {
  /// Creates a new `ComparableDB` with the given options.
  #[inline]
  pub fn new() -> Self {
    Self::with_options(Default::default())
  }
}

impl<K, V> ComparableDB<K, V> {
  /// Creates a new `ComparableDB` with the given options.
  #[inline]
  pub fn with_options(opts: Options) -> Self {
    Self {
      inner: Arc::new(Inner::new(
        "ComparableDB",
        opts.max_batch_size,
        opts.max_batch_entries,
      )),
    }
  }

  /// Create a read transaction.
  #[inline]
  pub fn read(&self) -> ReadTransaction<K, V, ComparableDB<K, V>, BTreeCm<K>> {
    ReadTransaction::new(self.clone(), self.inner.tm.read())
  }
}
