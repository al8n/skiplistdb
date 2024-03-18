//! Blazing fast ACID and MVCC in memory database based on [`crossbeam-skiplist`](https://crates.io/crates/crossbeam-skiplist).
//!
//! `skiplistdb` uses the same SSI (Serializable Snapshot Isolation) transaction model used in [`badger`](https://github.com/dgraph-io/badger).
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use std::{
  borrow::Borrow,
  collections::BTreeMap,
  hash::BuildHasher,
  iter::FusedIterator,
  ops::{Bound, RangeBounds},
  sync::Arc,
};

use crossbeam_skiplist::{
  map::{Entry as MapEntry, Range as MapRange},
  SkipMap,
};
use either::Either;
use mwmr::{
  error::TransactionError, EntryValue, HashCm, OneOrMore, Pwm, PwmComparable, Rtm, Tm, Wtm,
};

/// `EquivalentDB` implementation, which requires `K` implements both [`Hash`](core::hash::Hash) and [`Ord`].
/// If your `K` does not implement [`Hash`](core::hash::Hash), you can use [`ComparableDB`] instead.
pub mod equivalent;

/// `ComparableDB` implementation, which requires `K` implements [`Ord`] and [`CheapClone`](cheap_clone::CheapClone). If your `K` implements both [`Hash`](core::hash::Hash) and [`Ord`], you are recommended to use [`EquivalentDB`](crate::equivalent::EquivalentDB) instead.
pub mod comparable;

mod iter;
pub use iter::*;
mod range;
pub use range::*;
mod read;
pub use read::*;

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

/// Pending write manger implementation for [`EquivalentDB`] and [`ComparableDB`].
pub struct PendingMap<K, V> {
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

  fn iter(&self) -> impl Iterator<Item = (&Self::Key, &EntryValue<Self::Value>)> {
    self.map.iter()
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
    Q: Ord + ?Sized,
  {
    Ok(self.map.get(key))
  }

  fn get_entry_comparable<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(self.map.get_key_value(key))
  }

  fn contains_key_comparable<Q>(&self, key: &Q) -> Result<bool, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(self.map.contains_key(key))
  }

  fn remove_entry_comparable<Q>(
    &mut self,
    key: &Q,
  ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(self.map.remove_entry(key))
  }
}

/// A reference to an entry in the write transaction.
pub struct Entry<'a, K, V> {
  ent: MapEntry<'a, u64, Option<V>>,
  key: &'a K,
  version: u64,
}

impl<'a, K, V> Clone for Entry<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      version: self.version,
      key: self.key,
    }
  }
}

impl<'a, K, V> Entry<'a, K, V> {
  /// Get the value of the entry.
  #[inline]
  pub fn value(&self) -> &V {
    self.ent.value().as_ref().unwrap()
  }

  /// Get the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K {
    self.key
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

/// A reference to an entry in the write transaction.
pub struct Ref<'a, K, V> {
  ent: MapEntry<'a, K, SkipMap<u64, Option<V>>>,
  version: u64,
}

impl<'a, K, V> Clone for Ref<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      version: self.version,
    }
  }
}

impl<'a, K, V> Ref<'a, K, V> {
  /// Get the value of the entry.
  #[inline]
  pub fn entry(&self) -> Entry<'_, K, V> {
    let ent = self.ent.value().get(&self.version).unwrap();

    Entry {
      ent,
      key: self.ent.key(),
      version: self.version,
    }
  }

  /// Get the key of the ref.
  #[inline]
  pub fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

/// A reference to an entry in the write transaction.
pub struct OptionRef<'a, K, V> {
  ent: MapEntry<'a, K, SkipMap<u64, Option<V>>>,
  version: u64,
}

impl<'a, K, V> Clone for OptionRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      version: self.version,
    }
  }
}

impl<'a, K, V> OptionRef<'a, K, V> {
  /// Get the value of the entry. `None` this entry is marked as removed.
  #[inline]
  pub fn entry(&self) -> Option<Entry<'_, K, V>> {
    let ent = self.ent.value().get(&self.version).unwrap();

    if ent.value().is_some() {
      return Some(Entry {
        ent,
        key: self.ent.key(),
        version: self.version,
      });
    }

    None
  }

  /// Get the key of the ref.
  #[inline]
  pub fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

/// A reference to an entry in the write transaction.
pub struct PendingRef<'a, K, V> {
  ent: mwmr::EntryRef<'a, K, V>,
}

impl<'a, K, V> Clone for PendingRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, K, V> Copy for PendingRef<'a, K, V> {}

impl<'a, K, V> PendingRef<'a, K, V> {
  #[inline]
  const fn new(ent: mwmr::EntryRef<'a, K, V>) -> Self {
    Self { ent }
  }

  /// Get the value of the entry.
  #[inline]
  pub fn value(&self) -> &V {
    self.ent.value().unwrap()
  }

  /// Get the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.ent.version()
  }
}

/// A reference to an entry in the write transaction. If the value is None, it means that the key is removed.
pub struct OptionPendingRef<'a, K, V> {
  ent: mwmr::EntryRef<'a, K, V>,
}

impl<'a, K, V> Clone for OptionPendingRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, K, V> Copy for OptionPendingRef<'a, K, V> {}

impl<'a, K, V> OptionPendingRef<'a, K, V> {
  /// Create a new entry reference.
  #[inline]
  const fn new(ent: mwmr::EntryRef<'a, K, V>) -> Self {
    Self { ent }
  }

  /// Get the value of the entry.
  #[inline]
  pub fn value(&self) -> Option<&V> {
    self.ent.value()
  }

  /// Get the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.ent.version()
  }
}

#[doc(hidden)]
pub trait Database<K, V>: sealed::AsInnerDB<K, V> {}

impl<K, V, T: sealed::AsInnerDB<K, V>> Database<K, V> for T {}

mod sealed {
  #[doc(hidden)]
  pub trait AsInnerDB<K, V> {
    // This trait is sealed and cannot be implemented for types outside of this crate.
    // So returning a reference to the inner database is ok.
    #[allow(private_interfaces)]
    fn as_inner(&self) -> &super::InnerDB<K, V>;
  }
}

struct InnerDB<K, V>(SkipMap<K, SkipMap<u64, Option<V>>>);

impl<K, V> InnerDB<K, V>
where
  K: Ord,
  V: Send + 'static,
{
  fn apply(&self, entries: OneOrMore<mwmr::Entry<K, V>>) {
    for ent in entries {
      let version = ent.version();
      match ent.data {
        mwmr::EntryData::Insert { key, value } => {
          let values = self.0.get_or_insert(key, SkipMap::new());
          values.value().insert(version, Some(value));
        }
        mwmr::EntryData::Remove(key) => {
          if let Some(values) = self.0.get(&key) {
            let values = values.value();
            if !values.is_empty() {
              values.insert(version, None);
            }
          }
        }
      }
    }
  }
}

impl<K, V> InnerDB<K, V>
where
  K: Ord,
{
  fn get<Q>(&self, key: &Q, version: u64) -> Option<Ref<'_, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let ent = self.0.get(key)?;
    let version = ent
      .value()
      .upper_bound(Bound::Included(&version))
      .and_then(|v| {
        if v.value().is_some() {
          Some(*v.key())
        } else {
          None
        }
      })?;

    Some(Ref { ent, version })
  }

  fn contains_key<Q>(&self, key: &Q, version: u64) -> bool
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    match self.0.get(key) {
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
    self.0.get(key).and_then(move |values| {
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

  fn iter(&self, version: u64) -> Iter<'_, K, V> {
    let iter = self.0.iter();
    Iter { iter, version }
  }

  fn iter_all_versions(&self, version: u64) -> AllVersionsIter<'_, K, V> {
    let iter = self.0.iter();
    AllVersionsIter { iter, version }
  }

  fn range<Q, R>(&self, range: R, version: u64) -> Range<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    Range {
      range: self.0.range(range),
      version,
    }
  }

  fn range_all_versions<Q, R>(&self, range: R, version: u64) -> AllVersionsRange<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    AllVersionsRange {
      range: self.0.range(range),
      version,
    }
  }
}
