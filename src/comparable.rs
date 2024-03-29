use std::sync::Arc;

pub use cheap_clone::CheapClone;
use crossbeam_skiplist::SkipMap;
use mwmr::{BTreeCm, Tm};

use super::{InnerDB, Options, PendingMap, ReadTransaction};

mod write;
pub use write::*;

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
///
/// `ComparableDB` requires key to be [`Ord`] and [`CheapClone`].
/// The [`CheapClone`] bound here hints the user that the key should be cheap to clone,
/// because it will be cloned at least one time during the write transaction.
///
/// Comparing to [`EquivalentDB`](crate::equivalent::EquivalentDB), `ComparableDB` does not require key to implement [`Hash`](core::hash::Hash).
/// But, [`EquivalentDB`](crate::equivalent::EquivalentDB) has more flexible write transaction APIs.
pub struct ComparableDB<K, V> {
  inner: Arc<Inner<K, V>>,
}

impl<K, V> super::sealed::AsInnerDB<K, V> for ComparableDB<K, V> {
  #[inline]
  #[allow(private_interfaces)]
  fn as_inner(&self) -> &InnerDB<K, V> {
    &self.inner.map
  }
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
        core::any::type_name::<Self>(),
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

impl<K, V> ComparableDB<K, V>
where
  K: CheapClone + Ord + 'static,
  V: 'static,
{
  /// Create a write transaction.
  #[inline]
  pub fn write(&self) -> WriteTransaction<K, V> {
    WriteTransaction::new(self.clone())
  }
}
