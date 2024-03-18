use super::*;

use std::{cmp, collections::btree_map::Iter as BTreeMapIter};

use mwmr::{Marker, OneOrMore};

mod write;
pub use write::*;

/// Iterators
pub mod iter;

/// Subset iterators
pub mod range;

struct Inner<K, V, S = std::hash::DefaultHasher> {
  tm: Tm<K, V, HashCm<K, S>, PendingMap<K, V>>,
  map: InnerDB<K, V>,
  hasher: S,
  max_batch_size: u64,
  max_batch_entries: u64,
}

impl<K, V, S> Inner<K, V, S> {
  fn new(name: &str, max_batch_size: u64, max_batch_entries: u64, hasher: S) -> Self {
    let map = SkipMap::new();
    let tm = Tm::new(name, 0);
    Self {
      tm,
      map: InnerDB(map),
      hasher,
      max_batch_size,
      max_batch_entries,
    }
  }
}

/// A concurrent ACID, MVCC in-memory database based on [`crossbeam-skiplist`][crossbeam_skiplist].
pub struct EquivalentDB<K, V, S = std::hash::DefaultHasher> {
  inner: Arc<Inner<K, V, S>>,
}

impl<K, V, S> super::sealed::AsInnerDB<K, V> for EquivalentDB<K, V, S> {
  #[inline]
  #[allow(private_interfaces)]
  fn as_inner(&self) -> &InnerDB<K, V> {
    &self.inner.map
  }
}

impl<K, V, S> Clone for EquivalentDB<K, V, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V> Default for EquivalentDB<K, V> {
  /// Creates a new `EquivalentDB` with the default options.
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> EquivalentDB<K, V> {
  /// Creates a new `EquivalentDB` with the given options.
  #[inline]
  pub fn new() -> Self {
    Self::with_options_and_hasher(Default::default(), Default::default())
  }
}

impl<K, V, S> EquivalentDB<K, V, S> {
  /// Creates a new `EquivalentDB` with the given hasher.
  #[inline]
  pub fn with_hasher(hasher: S) -> Self {
    Self::with_options_and_hasher(Default::default(), hasher)
  }

  /// Creates a new `EquivalentDB` with the given options and hasher.
  #[inline]
  pub fn with_options_and_hasher(opts: Options, hasher: S) -> Self {
    let inner = Arc::new(Inner::new(
      "skiplistdb",
      opts.max_batch_size,
      opts.max_batch_entries,
      hasher,
    ));
    Self { inner }
  }

  /// Create a read transaction.
  #[inline]
  pub fn read(&self) -> ReadTransaction<K, V, EquivalentDB<K, V, S>, HashCm<K, S>> {
    ReadTransaction::new(self.clone(), self.inner.tm.read())
  }
}

impl<K, V, S> EquivalentDB<K, V, S>
where
  K: Ord + Eq + core::hash::Hash + 'static,
  V: 'static,
  S: BuildHasher + Clone + 'static,
{
  /// Create a write transaction.
  #[inline]
  pub fn write(&self) -> WriteTransaction<K, V, S> {
    WriteTransaction::new(self.clone())
  }
}
