//! Blazing fast ACID and MVCC in memory database based on [`crossbeam-skiplist`](https://crates.io/crates/crossbeam-skiplist).
//!
//! `skiplistdb` uses the same SSI (Serializable Snapshot Isolation) transaction model used in [`badger`](https://github.com/dgraph-io/badger).
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use std::{
  borrow::Borrow,
  collections::{BTreeMap, HashSet},
  hash::{BuildHasher, DefaultHasher},
  iter::FusedIterator,
  mem,
  ops::{Bound, RangeBounds},
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
};

use crossbeam_skiplist::{
  map::{Entry as MapEntry, Range as MapRange},
  SkipMap,
};
use either::Either;
use mwmr::{
  error::TransactionError, BTreeMapManager, Cm, EntryData, EntryValue, HashCm, Pwm, PwmComparable, Rtm, Tm, Wtm
};

/// `EquivalentDB` implementation, which requires `K` implements both [`Hash`](core::hash::Hash) and [`Ord`].
/// If your `K` does not implement [`Hash`](core::hash::Hash), you can use [`ComparableDB`] instead.
pub mod equivalent;

/// `ComparableDB` implementation, which requires `K` implements [`Ord`]. If your `K` implements both [`Hash`](core::hash::Hash) and [`Ord`], you are recommended to use [`EquivalentDB`] instead.
pub mod comparable;

/// The options used to create a new `EquivalentDB`.
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

  fn contains_key(&self, key: &Self::Key) -> Result<bool, Self::Error> {
    Ok(self.map.contains_key(key))
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
    core::iter::IntoIterator::into_iter(self.map)
  }
}


impl<K, V> PwmComparable for PendingMap<K, V>
where
  K: Ord + 'static,
  V: 'static,
{
  fn get_comparable<Q>(&self, key: &Q) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized
  {
    Ok(self.map.get(key))
  }

  fn get_entry_comparable<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized
  {
    Ok(self.map.get_key_value(key))
  }

  fn contains_key_comparable<Q>(&self, key: &Q) -> Result<bool, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized
  {
    Ok(self.map.contains_key(key))
  }

  fn remove_entry_comparable<Q>(
    &mut self,
    key: &Q,
  ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized
  {
    Ok(self.map.remove_entry(key))
  }
}
