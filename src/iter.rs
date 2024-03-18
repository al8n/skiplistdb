use super::*;

/// An iterator over a all values with the same key in different versions.
pub struct AllVersions<'a, K, V> {
  pub(crate) max_version: u64,
  pub(crate) min_version: u64,
  pub(crate) current_version: u64,
  pub(crate) entries: MapEntry<'a, K, SkipMap<u64, Option<V>>>,
}

impl<'a, K, V> AllVersions<'a, K, V> {
  /// Returns the key of the entries.
  pub fn key(&self) -> &K {
    self.entries.key()
  }
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
  type Item = OptionRef<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
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

        OptionRef {
          ent: self.entries.clone(),
          version: ent_version,
        }
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

        OptionRef {
          ent: self.entries.clone(),
          version: self.current_version,
        }
      })
  }
}

impl<'a, K, V> FusedIterator for AllVersions<'a, K, V> {}

/// An iterator over a all values with the same key in different versions.
pub struct WriteTransactionAllVersions<'a, K, V> {
  pub(crate) pending: Option<OptionPendingRef<'a, K, V>>,
  pub(crate) committed: Option<AllVersions<'a, K, V>>,
}

impl<'a, K, V> Clone for WriteTransactionAllVersions<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      pending: self.pending,
      committed: self.committed.clone(),
    }
  }
}

impl<'a, K, V> Iterator for WriteTransactionAllVersions<'a, K, V> {
  type Item = Either<OptionPendingRef<'a, K, V>, OptionRef<'a, K, V>>;

  fn next(&mut self) -> Option<Self::Item> {
    if let Some(p) = self.pending.take() {
      return Some(Either::Left(p));
    }

    if let Some(committed) = &mut self.committed {
      committed.next().map(Either::Right)
    } else {
      None
    }
  }

  fn last(mut self) -> Option<Self::Item>
  where
    Self: Sized,
  {
    if let Some(committed) = self.committed.take() {
      return committed.last().map(Either::Right);
    }

    self.pending.take().map(Either::Left)
  }
}

impl<'a, K, V> FusedIterator for WriteTransactionAllVersions<'a, K, V> {}

/// An iterator over the entries of the database.
pub struct Iter<'a, K, V> {
  pub(crate) iter: crossbeam_skiplist::map::Iter<'a, K, SkipMap<u64, Option<V>>>,
  pub(crate) version: u64,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
  K: Ord,
{
  type Item = Ref<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next()?;
      if let Some(version) = ent
        .value()
        .upper_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(Ref { version, ent });
      }
    }
  }
}

impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V>
where
  K: Ord,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next_back()?;
      if let Some(version) = ent
        .value()
        .lower_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(Ref { version, ent });
      }
    }
  }
}

/// An iterator over the entries of the database.
pub struct AllVersionsIter<'a, K, V> {
  pub(crate) iter: crossbeam_skiplist::map::Iter<'a, K, SkipMap<u64, Option<V>>>,
  pub(crate) version: u64,
}

impl<'a, K, V> Iterator for AllVersionsIter<'a, K, V>
where
  K: Ord,
{
  type Item = AllVersions<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next()?;
      let min = *ent.value().front().unwrap().key();
      if let Some(version) = ent
        .value()
        .upper_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(AllVersions {
          max_version: version,
          min_version: min,
          current_version: version,
          entries: ent,
        });
      }
    }
  }
}

impl<'a, K, V> DoubleEndedIterator for AllVersionsIter<'a, K, V>
where
  K: Ord,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next_back()?;
      let min = *ent.value().front().unwrap().key();
      if let Some(version) = ent
        .value()
        .lower_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(AllVersions {
          max_version: version,
          min_version: min,
          current_version: version,
          entries: ent,
        });
      }
    }
  }
}
