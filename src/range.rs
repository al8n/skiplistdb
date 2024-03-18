use super::*;

/// An iterator over a subset of entries of the database.
pub struct Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  pub(crate) range: MapRange<'a, Q, R, K, SkipMap<u64, Option<V>>>,
  pub(crate) version: u64,
}

impl<'a, Q, R, K, V> Iterator for Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  type Item = Ref<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next()?;
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

impl<'a, Q, R, K, V> DoubleEndedIterator for Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next_back()?;
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

/// An iterator over a subset of entries of the database.
pub struct AllVersionsRange<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  pub(crate) range: MapRange<'a, Q, R, K, SkipMap<u64, Option<V>>>,
  pub(crate) version: u64,
}

impl<'a, Q, R, K, V> Iterator for AllVersionsRange<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  type Item = AllVersions<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next()?;
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

impl<'a, Q, R, K, V> DoubleEndedIterator for AllVersionsRange<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next_back()?;
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
