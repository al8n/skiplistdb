//! Blazing fast ACID and MVCC in memory database based on [`crossbeam-skiplist`](https://crates.io/crates/crossbeam-skiplist).
//!
//! `skiplistdb` uses the same SSI (Serializable Snapshot Isolation) transaction model used in [`badger`](https://github.com/dgraph-io/badger).
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use std::{
  borrow::Borrow, collections::{BTreeMap, HashSet}, hash::{BuildHasher, DefaultHasher}, iter::FusedIterator, mem, ops::{Bound, RangeBounds}, sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  }
};

use crossbeam_skiplist::{
  map::{Entry as MapEntry, Range as MapRange},
  SkipMap,
};
use either::Either;
use mwmr::{error::TransactionError, BTreeMapManager, Cm, EntryData, EntryValue, HashCm, Pwm, Rtm, Tm, Wtm};

mod read;
pub use read::*;

mod write;
pub use write::*;

/// The options used to create a new `SkipListDB`.
#[derive(Debug, Clone)]
pub struct Options {
  max_batch_size: u64,
  max_batch_entries: u64,
  detect_conflicts: bool,
}

impl Default for Options {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl Options {
  /// Creates a new `Options` with the default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      max_batch_size: u64::MAX,
      max_batch_entries: u64::MAX,
      detect_conflicts: true,
    }
  }

  /// Sets the maximum batch size in bytes.
  #[inline]
  pub fn with_max_batch_size(mut self, max_batch_size: u64) -> Self {
    self.max_batch_size = max_batch_size;
    self
  }

  /// Sets the maximum entries in batch.
  #[inline]
  pub fn with_max_batch_entries(mut self, max_batch_entries: u64) -> Self {
    self.max_batch_entries = max_batch_entries;
    self
  }

  /// Sets the detect conflicts.
  #[inline]
  pub fn with_detect_conflicts(mut self, detect_conflicts: bool) -> Self {
    self.detect_conflicts = detect_conflicts;
    self
  }

  /// Returns the maximum batch size in bytes.
  #[inline]
  pub const fn max_batch_size(&self) -> u64 {
    self.max_batch_size
  }

  /// Returns the maximum entries in batch.
  #[inline]
  pub const fn max_batch_entries(&self) -> u64 {
    self.max_batch_entries
  }

  /// Returns the detect conflicts.
  #[inline]
  pub const fn detect_conflicts(&self) -> bool {
    self.detect_conflicts
  }
}

struct PendingMap<K, V> {
  map: BTreeMap<K, EntryValue<V>>,
  opts: Options,
}

impl<K: Clone, V: Clone> Clone for PendingMap<K, V> {
  fn clone(&self) -> Self {
    Self {
      map: self.map.clone(),
      opts: self.opts.clone(),
    }
  }
}

impl<K, V> PendingMap<K, V>
where
  K: Ord + 'static,
  V: 'static,
{
  fn get_by_borrow<Q>(&self, key: &Q) -> Option<&EntryValue<V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.map.get(key)
  }

  fn contains_by_borrow<Q>(&self, key: &Q) -> bool
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.map.contains_key(key)
  }
}

impl<K, V> Pwm for PendingMap<K, V>
where
  K: Ord + 'static,
  V: 'static,
{
  type Error = core::convert::Infallible;

  type Key = K;

  type Value = V;

  type Options = Options;

  fn new(options: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      map: BTreeMap::new(),
      opts: options,
    })
  }

  fn is_empty(&self) -> bool {
    self.map.is_empty()
  }

  fn len(&self) -> usize {
    self.map.len()
  }

  fn validate_entry(
    &self,
    _entry: &mwmr::Entry<Self::Key, Self::Value>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  fn max_batch_size(&self) -> u64 {
    self.opts.max_batch_size
  }

  fn max_batch_entries(&self) -> u64 {
    self.opts.max_batch_entries
  }

  fn estimate_size(&self, _entry: &mwmr::Entry<Self::Key, Self::Value>) -> u64 {
    core::mem::size_of::<Self::Key>() as u64 + core::mem::size_of::<Self::Value>() as u64
  }

  fn get(&self, key: &Self::Key) -> Result<Option<&EntryValue<Self::Value>>, Self::Error> {
    Ok(self.map.get(key))
  }

  fn insert(&mut self, key: Self::Key, value: EntryValue<Self::Value>) -> Result<(), Self::Error> {
    self.map.insert(key, value);
    Ok(())
  }

  fn remove_entry(
    &mut self,
    key: &Self::Key,
  ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error> {
    Ok(self.map.remove_entry(key))
  }

  fn into_iter(self) -> impl Iterator<Item = (Self::Key, EntryValue<Self::Value>)> {
    self.map.into_iter()
  }
}

struct Inner<K, V, S = std::hash::DefaultHasher> {
  tm: Tm<K, V, HashCm<K, S>, PendingMap<K, V>>,
  map: SkipMap<K, SkipMap<u64, Option<Arc<V>>>>,
  hasher: S,
  max_batch_size: u64,
  max_batch_entries: u64,
  len: AtomicUsize,
  len_including_versions: AtomicUsize,
}

impl<K, V, S> Inner<K, V, S> {
  fn new(name: &str, max_batch_size: u64, max_batch_entries: u64, hasher: S) -> Self {
    let map = SkipMap::new();
    let tm = Tm::new(name, 0);
    Self {
      tm,
      map,
      hasher,
      max_batch_size,
      max_batch_entries,
      len: AtomicUsize::new(0),
      len_including_versions: AtomicUsize::new(0),
    }
  }
}

/// A concurrent ACID, MVCC in-memory database based on [`crossbeam-skiplist`][crossbeam_skiplist].
pub struct SkipListDB<K, V, S = std::hash::DefaultHasher> {
  inner: Arc<Inner<K, V, S>>,
}

impl<K, V, S> Clone for SkipListDB<K, V, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V> Default for SkipListDB<K, V> {
  /// Creates a new `SkipListDB` with the default options.
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> SkipListDB<K, V> {
  /// Creates a new `SkipListDB` with the given options.
  #[inline]
  pub fn new() -> Self {
    Self::with_options_and_hasher(Default::default(), Default::default())
  }
}

impl<K, V, S> SkipListDB<K, V, S> {
  /// Creates a new `SkipListDB` with the given hasher.
  #[inline]
  pub fn with_hasher(hasher: S) -> Self {
    Self::with_options_and_hasher(Default::default(), hasher)
  }

  /// Creates a new `SkipListDB` with the given options and hasher.
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
  pub fn read(&self) -> ReadTransaction<K, V, S> {
    ReadTransaction::new(self.clone())
  }

  /// Create a write transaction.
  #[inline]
  pub fn write(&self) -> WriteTransaction<K, V, S> {
    WriteTransaction::new(self.clone())
  }
}

impl<K, V, S> SkipListDB<K, V, S>
where
  K: Ord,
{
  fn get<Q>(&self, key: &Q, version: u64) -> Option<Arc<V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let ent = self.inner.map.get(key)?;
    ent
      .value()
      .upper_bound(Bound::Included(&version))
      .and_then(|vent| vent.value().clone())
  }

  fn contains_key<Q>(&self, key: &Q, version: u64) -> bool
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    match self.inner.map.get(key) {
      None => false,
      Some(values) => values
        .value()
        .upper_bound(Bound::Included(&version))
        .is_some(),
    }
  }

  fn get_all_versions<'a, 'b: 'a, Q>(
    &'a self,
    key: &'b Q,
    version: u64,
  ) -> Option<AllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.inner.map.get(key).and_then(move |values| {
      let ents = values.value();
      if ents.is_empty() {
        return None;
      }

      let min = *ents.front().unwrap().key();
      if min > version {
        return None;
      }

      Some(AllVersions {
        max_version: version,
        min_version: min,
        current_version: version,
        entries: values,
      })
    })
  }
}

/// An iterator over a all values with the same key in different versions.
pub struct AllVersions<'a, K, V> {
  max_version: u64,
  min_version: u64,
  current_version: u64,
  entries: MapEntry<'a, K, SkipMap<u64, Option<Arc<V>>>>,
}

impl<'a, K, V> Clone for AllVersions<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      max_version: self.max_version,
      min_version: self.min_version,
      current_version: self.current_version,
      entries: self.entries.clone(),
    }
  }
}

impl<'a, K, V> Iterator for AllVersions<'a, K, V> {
  type Item = Option<Arc<V>>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.current_version >= self.max_version {
      return None;
    }

    self
      .entries
      .value()
      .upper_bound(Bound::Included(&self.current_version))
      .map(|ent| {
        let ent_version = *ent.key();

        if self.current_version != ent_version {
          self.current_version = ent_version;
        } else {
          self.current_version += 1;
        }

        ent.value().clone()
      })
  }

  fn last(mut self) -> Option<Self::Item>
  where
    Self: Sized,
  {
    self
      .entries
      .value()
      .lower_bound(Bound::Included(&self.max_version))
      .map(|ent| {
        self.current_version = *ent.key();
        ent.value().clone()
      })
  }
}

impl<'a, K, V> FusedIterator for AllVersions<'a, K, V> {}


/// An iterator over a all values with the same key in different versions.
pub struct AllVersionsWithPending<'a, K, V> {
  pending: Option<Arc<V>>,
  committed: Option<AllVersions<'a, K, V>>,
}

impl<'a, K, V> Clone for AllVersionsWithPending<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      pending: self.pending.clone(),
      committed: self.committed.clone(),
    }
  }
}

impl<'a, K, V> Iterator for AllVersionsWithPending<'a, K, V> {
  type Item = Option<Arc<V>>;

  fn next(&mut self) -> Option<Self::Item> {
    if let Some(p) = self.pending.take() {
      return Some(Some(p));
    }

    if let Some(committed) = &mut self.committed {
      committed.next()
    } else {
      None
    }
  }

  fn last(mut self) -> Option<Self::Item>
  where
    Self: Sized, {
    if let Some(committed) = self.committed.take() {
      return committed.last();
    }

    self.pending.take().map(Some)
  }
}

impl<'a, K, V> FusedIterator for AllVersionsWithPending<'a, K, V> {}
