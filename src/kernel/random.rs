//! The deterministic random source for runs under the kernel's scheme.
//!
//! Workflow code is replayed, so every random value it draws must come out
//! the same on every replay, in every language, without being journaled. A
//! run's values come from one generator seeded from its history lineage:
//! reset copies the lineage, so a reset run draws the same values; a fresh
//! history has a new lineage, and new values.
//!
//! Every operation below is a precise algorithm, not "whatever the language's
//! library does", so an SDK that implements it natively and one that calls the
//! kernel produce the same values. `src/kernel/random_v1.json` holds the
//! values for fixed seeds; any implementation must reproduce them exactly.
//!
//! The algorithm, version 1:
//!
//! - **Seed**: SHA-256 of `orcher.kernel.v1.random`, a zero byte, and the
//!   lineage's UTF-8 bytes. The 32 bytes are the generator's state, four
//!   little-endian u64 words. (An all-zero state, which xoshiro cannot
//!   leave, is replaced by the first word set to 1. SHA-256 producing it is
//!   not expected; the rule only makes the algorithm total.)
//! - **Generator**: xoshiro256**, its `next` returning a u64.
//! - **Float** in [0, 1): `(next >> 11) * 2^-53`, exact in an IEEE double.
//! - **Below `n`** (n > 0), unbiased: `threshold = (2^64 - n) mod n`; draw until
//!   `x >= threshold`; return `x mod n`.
//! - **Range** `[low, high)` of i64: `low + below(high - low)`.
//! - **Bool**: the top bit of `next`.
//! - **Shuffle** in place: for `i` from `len - 1` down to 1, swap `i` with
//!   `below(i + 1)`.
//! - **Sample** `k` of `n` without replacement: indices `0..n`; for `i` in `0..k`,
//!   swap `i` with `i + below(n - i)`; the first `k` indices, in order.
//! - **UUID** (version 4): the little-endian bytes of two `next` values;
//!   byte 6 becomes `(b6 & 0x0f) | 0x40`, byte 8 `(b8 & 0x3f) | 0x80`.
//!
//! The algorithm is frozen: a different one is a new version, chosen per
//! run like the step-id scheme, never an edit here.

use sha2::{Digest, Sha256};

/// The domain label hashed with the lineage into the seed.
const SEED_LABEL: &[u8] = b"orcher.kernel.v1.random";

/// A run's deterministic random source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRandom {
    state: [u64; 4],
}

impl RunRandom {
    /// The source for the run whose history lineage is `lineage`.
    pub fn for_lineage(lineage: &str) -> Self {
        Self::from_seed(seed_for_lineage(lineage))
    }

    /// The source for a 32-byte seed (as `RunStarted.random_seed` carries it).
    pub fn from_seed(seed: [u8; 32]) -> Self {
        let mut state = [0u64; 4];
        for (word, bytes) in state.iter_mut().zip(seed.chunks_exact(8)) {
            *word = u64::from_le_bytes(bytes.try_into().expect("8 bytes"));
        }
        if state == [0; 4] {
            state[0] = 1;
        }
        Self { state }
    }

    /// The next 64 random bits (xoshiro256**).
    pub fn next_u64(&mut self) -> u64 {
        let [s0, s1, s2, s3] = &mut self.state;
        let result = s1.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = *s1 << 17;
        *s2 ^= *s0;
        *s3 ^= *s1;
        *s1 ^= *s2;
        *s0 ^= *s3;
        *s2 ^= t;
        *s3 = s3.rotate_left(45);
        result
    }

    /// A float in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// An integer in `[0, n)`, unbiased.
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0) has no value to return");
        let threshold = n.wrapping_neg() % n;
        loop {
            let x = self.next_u64();
            if x >= threshold {
                return x % n;
            }
        }
    }

    /// An integer in `[low, high)`.
    ///
    /// # Panics
    ///
    /// Panics if `low >= high`.
    pub fn range(&mut self, low: i64, high: i64) -> i64 {
        assert!(low < high, "range({low}, {high}) is empty");
        let span = high.wrapping_sub(low) as u64;
        low.wrapping_add(self.below(span) as i64)
    }

    /// A random bool.
    pub fn next_bool(&mut self) -> bool {
        self.next_u64() >> 63 == 1
    }

    /// Shuffles `items` in place.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            items.swap(i, j);
        }
    }

    /// `k` distinct indices of `n`, in the order drawn.
    ///
    /// # Panics
    ///
    /// Panics if `k > n`.
    pub fn sample(&mut self, n: usize, k: usize) -> Vec<usize> {
        assert!(k <= n, "cannot sample {k} of {n}");
        let mut indices: Vec<usize> = (0..n).collect();
        for i in 0..k {
            let j = i + self.below((n - i) as u64) as usize;
            indices.swap(i, j);
        }
        indices.truncate(k);
        indices
    }

    /// A version 4 UUID, as 16 bytes.
    pub fn uuid_bytes(&mut self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.next_u64().to_le_bytes());
        bytes[8..].copy_from_slice(&self.next_u64().to_le_bytes());
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        bytes
    }

    /// A version 4 UUID, hyphenated lowercase.
    pub fn uuid(&mut self) -> String {
        uuid::Uuid::from_bytes(self.uuid_bytes()).to_string()
    }
}

/// The seed of the run whose history lineage is `lineage`.
pub fn seed_for_lineage(lineage: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(SEED_LABEL);
    hasher.update([0u8]);
    hasher.update(lineage.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The values for fixed lineages, as `src/kernel/random_v1.json`
    /// records them: the contract every implementation is checked against.
    #[test]
    fn the_algorithm_reproduces_its_recorded_values() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("random_v1.json")).expect("the vectors are JSON");
        for case in vectors["cases"].as_array().expect("cases") {
            let lineage = case["lineage"].as_str().unwrap();
            assert_eq!(
                hex(&seed_for_lineage(lineage)),
                case["seed"].as_str().unwrap(),
                "seed for {lineage:?}"
            );
            assert_eq!(&generate(lineage), case, "values for {lineage:?}");
        }
    }

    /// xoshiro256**'s published reference outputs for the state [1, 2, 3, 4].
    #[test]
    fn the_generator_is_xoshiro256_star_star() {
        let mut random = RunRandom {
            state: [1, 2, 3, 4],
        };
        let outputs: Vec<u64> = (0..4).map(|_| random.next_u64()).collect();
        assert_eq!(outputs, [11520, 0, 1509978240, 1215971899390074240]);
    }

    #[test]
    fn a_reset_draws_the_same_values_and_a_fresh_history_others() {
        let mut original = RunRandom::for_lineage("lineage-a");
        let mut reset = RunRandom::for_lineage("lineage-a");
        let mut fresh = RunRandom::for_lineage("lineage-b");
        let drawn: Vec<u64> = (0..8).map(|_| original.next_u64()).collect();
        assert_eq!(drawn, (0..8).map(|_| reset.next_u64()).collect::<Vec<_>>());
        assert_ne!(drawn, (0..8).map(|_| fresh.next_u64()).collect::<Vec<_>>());
    }

    #[test]
    fn values_stay_in_their_bounds() {
        let mut random = RunRandom::for_lineage("bounds");
        for _ in 0..10_000 {
            let f = random.next_f64();
            assert!((0.0..1.0).contains(&f));
            assert!(random.below(7) < 7);
            assert!((-3..4).contains(&random.range(-3, 4)));
        }
        assert_eq!(random.range(i64::MIN, i64::MIN + 1), i64::MIN);
        let mut items: Vec<u32> = (0..50).collect();
        random.shuffle(&mut items);
        let mut sorted = items.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..50).collect::<Vec<_>>());
        let sample = random.sample(10, 4);
        assert_eq!(sample.len(), 4);
        assert!(sample.iter().all(|&i| i < 10));
        let mut unique = sample.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 4);
        let uuid = uuid::Uuid::parse_str(&random.uuid()).unwrap();
        assert_eq!(uuid.get_version_num(), 4);
    }

    /// What each case of the vectors file records, generated by this code.
    fn generate(lineage: &str) -> serde_json::Value {
        let mut r = RunRandom::for_lineage(lineage);
        let u64s: Vec<String> = (0..4).map(|_| r.next_u64().to_string()).collect();
        let floats: Vec<f64> = (0..4).map(|_| r.next_f64()).collect();
        let below_10: Vec<u64> = (0..6).map(|_| r.below(10)).collect();
        let range: Vec<i64> = (0..4).map(|_| r.range(-50, 50)).collect();
        let bools: Vec<bool> = (0..6).map(|_| r.next_bool()).collect();
        let mut shuffled: Vec<u32> = (0..10).collect();
        r.shuffle(&mut shuffled);
        let sample = r.sample(20, 5);
        let uuid = r.uuid();
        serde_json::json!({
            "lineage": lineage,
            "seed": hex(&seed_for_lineage(lineage)),
            "next_u64": u64s,
            "next_f64": floats,
            "below_10": below_10,
            "range_minus50_50": range,
            "next_bool": bools,
            "shuffle_0_to_9": shuffled,
            "sample_5_of_20": sample,
            "uuid": uuid,
        })
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Prints the vectors file. Run with `--ignored --nocapture` only when
    /// creating the file for a new version; version 1's must never change.
    #[test]
    #[ignore]
    fn print_vectors() {
        let cases: Vec<serde_json::Value> =
            ["", "lineage-a", "0b7e3c1a-5f2d-4c8e-9a6b-3d1f7e2c4b90"]
                .iter()
                .map(|lineage| generate(lineage))
                .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "algorithm": "orcher.kernel.v1.random",
                "note": "u64 values are decimal strings; every implementation must reproduce every value exactly",
                "cases": cases,
            }))
            .unwrap()
        );
    }
}
