use self::{
  iter::{AllVersions, AllVersionsIter, Iter},
  range::{AllVersionsRange, Range},
};

use super::*;

use std::{cmp, collections::btree_map::Iter as BTreeMapIter};

mod read;
use mwmr::{Marker, OneOrMore};
pub use read::*;

mod write;
pub use write::*;

/// Iterators
pub mod iter;

/// Subset iterators
pub mod range;

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

struct Inner<K, V, S = std::hash::DefaultHasher> {
  tm: Tm<K, V, HashCm<K, S>, PendingMap<K, V>>,
  map: SkipMap<K, SkipMap<u64, Option<V>>>,
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
      map,
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
  pub fn read(&self) -> ReadTransaction<K, V, S> {
    ReadTransaction::new(self.clone())
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

impl<K, V, S> EquivalentDB<K, V, S>
where
  K: Ord,
{
  fn apply(&self, entries: OneOrMore<mwmr::Entry<K, V>>,) {
    for ent in entries {
      match ent.data {
        mwmr::EntryData::Insert { key, value } => {}

        
      }
      let version = ent.version();
      self.inner.map.get_or_insert(key, value)
    }
  }

  fn get<Q>(&self, key: &Q, version: u64) -> Option<Ref<'_, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let ent = self.inner.map.get(key)?;
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

  fn iter(&self, version: u64) -> Iter<'_, K, V> {
    let iter = self.inner.map.iter();
    Iter { iter, version }
  }

  fn iter_all_versions(&self, version: u64) -> AllVersionsIter<'_, K, V> {
    let iter = self.inner.map.iter();
    AllVersionsIter { iter, version }
  }

  fn range<Q, R>(&self, range: R, version: u64) -> Range<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    Range {
      range: self.inner.map.range(range),
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
      range: self.inner.map.range(range),
      version,
    }
  }
}
