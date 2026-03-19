//! ZSet implementation: skiplist + dict, same as Redis.
//! - dict: member -> (score, node_index) for O(1) ZSCORE, O(log n) ZREM
//! - skiplist: ordered by (score, member) for ZRANGE, ZRANK, ZPOPMIN/MAX

use std::collections::HashMap;
use std::cmp::Ordering;

const MAX_LEVEL: usize = 32;
const HEAD_IDX: usize = 0;

/// Compare (score, member) for ZSet ordering. Same as Redis: score first, then member lex.
fn cmp_score_member(score_a: f64, member_a: &[u8], score_b: f64, member_b: &[u8]) -> Ordering {
    if score_a.is_nan() || score_b.is_nan() {
        return Ordering::Equal;
    }
    match score_a.partial_cmp(&score_b) {
        Some(Ordering::Equal) => member_a.cmp(member_b),
        Some(o) => o,
        None => Ordering::Equal,
    }
}

/// Skip list node. Stored in arena; indices used as pointers.
#[derive(Clone)]
struct SkipNode {
    member: Vec<u8>,
    score: f64,
    backward: Option<usize>,
    forward: Vec<Option<usize>>,
}

impl SkipNode {
    fn new(member: Vec<u8>, score: f64, level: usize) -> Self {
        Self {
            member,
            score,
            backward: None,
            forward: vec![None; level],
        }
    }

    /// Head node: empty member, score = NEG_INFINITY, max level forward ptrs.
    fn head() -> Self {
        Self {
            member: Vec::new(),
            score: f64::NEG_INFINITY,
            backward: None,
            forward: vec![None; MAX_LEVEL],
        }
    }
}

/// ZSet: skiplist + dict. Same structure as Redis.
#[derive(Clone)]
pub struct ZSet {
    /// Arena: nodes[0] = head, nodes[i] = real nodes.
    nodes: Vec<SkipNode>,
    /// Free list for reclaimed indices.
    free: Vec<usize>,
    /// dict: member -> node index.
    dict: HashMap<Vec<u8>, usize>,
    /// Number of elements (excluding head).
    len: usize,
}

impl ZSet {
    pub fn new() -> Self {
        let mut nodes = Vec::with_capacity(64);
        nodes.push(SkipNode::head());
        Self {
            nodes,
            free: Vec::new(),
            dict: HashMap::new(),
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get score by member. O(1) via dict.
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        let idx = self.dict.get(member)?;
        Some(self.nodes[*idx].score)
    }

    /// Add or update (member, score). Returns true if added (new), false if updated.
    pub fn add(&mut self, member: Vec<u8>, score: f64) -> Result<bool, String> {
        if !score.is_finite() {
            return Err("ERR value is not a valid float".to_string());
        }
        if let Some(&old_idx) = self.dict.get(&member) {
            let old_score = self.nodes[old_idx].score;
            if (old_score - score).abs() < f64::EPSILON {
                return Ok(false);
            }
            self.remove_node(old_idx);
            self.dict.remove(&member);
        }
        let added = true;
        let level = self.random_level();
        let idx = self.alloc_node(member.clone(), score, level);
        self.dict.insert(member, idx);

        let mut update = vec![HEAD_IDX; MAX_LEVEL];
        let mut x = HEAD_IDX;
        for i in (0..MAX_LEVEL).rev() {
            while let Some(next) = self.nodes[x].forward.get(i).and_then(|o| *o) {
                if cmp_score_member(
                    score,
                    &self.nodes[idx].member,
                    self.nodes[next].score,
                    &self.nodes[next].member,
                ) == Ordering::Less
                {
                    break;
                }
                x = next;
            }
            update[i] = x;
        }

        for i in 0..level {
            let prev = update[i];
            let next = self.nodes[prev].forward[i];
            self.nodes[prev].forward[i] = Some(idx);
            self.nodes[idx].forward[i] = next;
            if let Some(n) = next {
                self.nodes[n].backward = Some(idx);
            }
        }
        self.nodes[idx].backward = Some(update[0]);
        if update[0] != HEAD_IDX {
            // we are not the first node
        }
        self.len += 1;
        Ok(added)
    }

    fn alloc_node(&mut self, member: Vec<u8>, score: f64, level: usize) -> usize {
        let mut node = SkipNode::new(member, score, level);
        node.forward.resize(MAX_LEVEL, None);
        let idx = if let Some(i) = self.free.pop() {
            self.nodes[i] = node;
            i
        } else {
            let i = self.nodes.len();
            self.nodes.push(node);
            i
        };
        idx
    }

    fn remove_node(&mut self, idx: usize) {
        let level = self.nodes[idx].forward.len();
        for i in 0..level {
            let prev = self.find_prev_at_level(idx, i);
            let next = self.nodes[idx].forward.get(i).and_then(|o| *o);
            self.nodes[prev].forward[i] = next;
            if i == 0 {
                if let Some(n) = next {
                    self.nodes[n].backward = Some(prev);
                }
            }
        }
        self.nodes[idx] = SkipNode::head();
        self.free.push(idx);
        self.len -= 1;
    }

    /// Find the node at level `lvl` whose forward[lvl] points to `idx`.
    fn find_prev_at_level(&self, idx: usize, lvl: usize) -> usize {
        let mut prev = HEAD_IDX;
        while let Some(next) = self.nodes[prev].forward.get(lvl).and_then(|o| *o) {
            if next == idx {
                return prev;
            }
            prev = next;
        }
        prev
    }

    fn random_level(&self) -> usize {
        let mut lvl = 1;
        while lvl < MAX_LEVEL && self.rand_fraction() < 0.25 {
            lvl += 1;
        }
        lvl
    }

    fn rand_fraction(&self) -> f64 {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hash, Hasher};
        let mut hasher = RandomState::new().build_hasher();
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        seed.hash(&mut hasher);
        (hasher.finish() % 10000) as f64 / 10000.0
    }

    /// Iterate (member, score) in ascending order.
    pub fn iter(&self) -> ZSetIter<'_> {
        let first = self.nodes[HEAD_IDX].forward[0];
        ZSetIter {
            nodes: &self.nodes,
            current: first,
        }
    }

    /// Remove member. Returns true if removed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        let Some(&idx) = self.dict.get(member) else {
            return false;
        };
        self.dict.remove(member);
        self.remove_node(idx);
        true
    }

    /// Pop and return the lowest-scoring member.
    pub fn pop_min(&mut self) -> Option<(Vec<u8>, f64)> {
        let idx = self.nodes[HEAD_IDX].forward[0]?;
        let member = self.nodes[idx].member.clone();
        let score = self.nodes[idx].score;
        self.dict.remove(&member);
        self.remove_node(idx);
        Some((member, score))
    }

    /// Pop and return the highest-scoring member.
    pub fn pop_max(&mut self) -> Option<(Vec<u8>, f64)> {
        let mut x = HEAD_IDX;
        while let Some(next) = self.nodes[x].forward[0] {
            x = next;
        }
        if x == HEAD_IDX {
            return None;
        }
        let member = self.nodes[x].member.clone();
        let score = self.nodes[x].score;
        self.dict.remove(&member);
        self.remove_node(x);
        Some((member, score))
    }

    /// Rank (0-based) of member in ascending order.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let idx = *self.dict.get(member)?;
        let mut rank = 0_usize;
        let mut x = self.nodes[HEAD_IDX].forward[0]?;
        loop {
            if x == idx {
                return Some(rank);
            }
            rank += 1;
            x = self.nodes[x].forward[0]?;
        }
    }

    /// Rank (0-based) of member in descending order.
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let r = self.rank(member)?;
        Some(self.len.saturating_sub(1).saturating_sub(r))
    }

    /// Range by rank [start, stop] inclusive. Uses Redis-style negative indices.
    pub fn range_by_rank(&self, start: i64, stop: i64) -> Vec<(Vec<u8>, f64)> {
        let (s, e) = match slice_indices(self.len, start, stop) {
            Some(p) => p,
            None => return Vec::new(),
        };
        let mut out = Vec::with_capacity((e - s + 1).min(256));
        let mut x = self.nodes[HEAD_IDX].forward[0];
        let mut i = 0_usize;
        while let Some(idx) = x {
            if i > e {
                break;
            }
            if i >= s {
                out.push((self.nodes[idx].member.clone(), self.nodes[idx].score));
            }
            i += 1;
            x = self.nodes[idx].forward.get(0).and_then(|o| *o);
        }
        out
    }

    /// Range by rank [start, stop] in reverse order.
    pub fn range_by_rank_rev(&self, start: i64, stop: i64) -> Vec<(Vec<u8>, f64)> {
        let mut out = self.range_by_rank(start, stop);
        out.reverse();
        out
    }

    /// Range by score. min/max inclusive unless ( prefix.
    pub fn range_by_score(&self, min: f64, min_excl: bool, max: f64, max_excl: bool) -> Vec<(Vec<u8>, f64)> {
        let mut out = Vec::new();
        let mut x = self.nodes[HEAD_IDX].forward[0];
        while let Some(idx) = x {
            let s = self.nodes[idx].score;
            let in_min = if min_excl { s > min } else { s >= min };
            let in_max = if max_excl { s < max } else { s <= max };
            if in_min && in_max {
                out.push((self.nodes[idx].member.clone(), s));
            }
            if s > max {
                break;
            }
            x = self.nodes[idx].forward.get(0).and_then(|o| *o);
        }
        out
    }

    /// Range by score in reverse.
    pub fn range_by_score_rev(&self, min: f64, min_excl: bool, max: f64, max_excl: bool) -> Vec<(Vec<u8>, f64)> {
        let mut out = self.range_by_score(min, min_excl, max, max_excl);
        out.reverse();
        out
    }

    /// Count members with score in [min, max].
    pub fn count_by_score(&self, min: f64, min_excl: bool, max: f64, max_excl: bool) -> usize {
        self.range_by_score(min, min_excl, max, max_excl).len()
    }

    /// Range by lex (member order). min/max: (excl, [inclusive. - and + for inf.
    pub fn range_by_lex(&self, min: &[u8], min_inc: bool, max: &[u8], max_inc: bool) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut x = self.nodes[HEAD_IDX].forward[0];
        while let Some(idx) = x {
            let m = &self.nodes[idx].member;
            let m_slice = m.as_slice();
            let in_min = match (min.is_empty(), min_inc) {
                (true, _) => true,
                (_, true) => m_slice >= min,
                (_, false) => m_slice > min,
            };
            let in_max = match (max.is_empty(), max_inc) {
                (true, _) => true,
                (_, true) => m_slice <= max,
                (_, false) => m_slice < max,
            };
            if in_min && in_max {
                out.push(m.clone());
            }
            if !max.is_empty() && m_slice > max {
                break;
            }
            x = self.nodes[idx].forward.get(0).and_then(|o| *o);
        }
        out
    }

    /// Range by lex reverse.
    pub fn range_by_lex_rev(&self, min: &[u8], min_inc: bool, max: &[u8], max_inc: bool) -> Vec<Vec<u8>> {
        let mut out = self.range_by_lex(min, min_inc, max, max_inc);
        out.reverse();
        out
    }

    /// Count by lex range.
    pub fn count_by_lex(&self, min: &[u8], min_inc: bool, max: &[u8], max_inc: bool) -> usize {
        self.range_by_lex(min, min_inc, max, max_inc).len()
    }

    /// Remove by rank range.
    pub fn remove_by_rank(&mut self, start: i64, stop: i64) -> usize {
        let to_remove = self.range_by_rank(start, stop);
        let n = to_remove.len();
        for (m, _) in to_remove {
            self.remove(&m);
        }
        n
    }

    /// Remove by score range.
    pub fn remove_by_score(&mut self, min: f64, min_excl: bool, max: f64, max_excl: bool) -> usize {
        let to_remove = self.range_by_score(min, min_excl, max, max_excl);
        let n = to_remove.len();
        for (m, _) in to_remove {
            self.remove(&m);
        }
        n
    }

    /// Remove by lex range.
    pub fn remove_by_lex(&mut self, min: &[u8], min_inc: bool, max: &[u8], max_inc: bool) -> usize {
        let to_remove = self.range_by_lex(min, min_inc, max, max_inc);
        let n = to_remove.len();
        for m in to_remove {
            self.remove(&m);
        }
        n
    }

    /// Increment score by delta. Returns new score.
    pub fn incr_by(&mut self, member: &[u8], delta: f64) -> Result<f64, String> {
        let current = self.score(member).unwrap_or(0.0);
        let next = current + delta;
        if !next.is_finite() {
            return Err("ERR value is not a valid float".to_string());
        }
        self.add(member.to_vec(), next)?;
        Ok(next)
    }
}

fn slice_indices(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let list_len = len as i64;
    let mut s = if start < 0 { list_len + start } else { start };
    let mut e = if end < 0 { list_len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e >= list_len {
        e = list_len - 1;
    }
    if s > e || s >= list_len || e < 0 {
        return None;
    }
    Some((s as usize, e as usize))
}

impl Default for ZSet {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ZSetIter<'a> {
    nodes: &'a [SkipNode],
    current: Option<usize>,
}

impl<'a> Iterator for ZSetIter<'a> {
    type Item = (Vec<u8>, f64);

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = self.nodes.get(idx)?;
        self.current = node.forward.get(0).and_then(|o| *o);
        Some((node.member.clone(), node.score))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zset_add_and_iter() {
        let mut z = ZSet::new();
        z.add(b"a".to_vec(), 1.0).unwrap();
        z.add(b"b".to_vec(), 2.0).unwrap();
        z.add(b"c".to_vec(), 3.0).unwrap();
        let collected: Vec<_> = z.iter().collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].0, b"a");
        assert_eq!(collected[0].1, 1.0);
    }
}
