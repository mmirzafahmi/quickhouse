//! What the machine this transfer is running on can actually offer.
//!
//! quickhouse's tuning defaults used to be fixed literals — `parallelism=4`,
//! `max_memory_bytes=512 MiB` — chosen without reference to the host. Both are
//! wrong in predictable ways. A fixed parallelism under-uses a big box and
//! oversubscribes a small one; a fixed byte ceiling is the wrong *unit*
//! entirely once several containers share a VM, because what matters is this
//! container's share and nothing here knows how many peers it has.
//!
//! Everything in this module is a best-effort probe with a documented fallback:
//! a transfer must still run when the host won't say.

/// CPUs available to this process, or 1 if the platform won't say.
///
/// `std::thread::available_parallelism` already accounts for cgroup CPU quota
/// on Linux, so inside a container this is the container's share rather than
/// the host's core count.
pub fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Memory this process is allowed to use, in bytes, or `None` if it can't be
/// determined.
///
/// Prefers the cgroup limit over total system memory, which is the whole point:
/// four 4 GiB containers on a 16 GiB VM each need to size against 4 GiB, and
/// `/proc/meminfo` would tell all four of them 16. Checks cgroup v2
/// (`memory.max`) then v1 (`memory.limit_in_bytes`), each of which may report
/// "unlimited" — as the literal `max` in v2, or an enormous sentinel in v1 —
/// in which case it falls through to `MemTotal`.
///
/// Non-Linux hosts return `None`; there is no portable equivalent, and a wrong
/// number is worse than no number for something used to size a ceiling.
pub fn memory_limit_bytes() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        if let Some(v) = cgroup_v2_limit().or_else(cgroup_v1_limit) {
            return Some(v);
        }
        mem_total()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn cgroup_v2_limit() -> Option<usize> {
    let raw = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    // "max" means no limit is set for this cgroup.
    raw.trim().parse::<usize>().ok()
}

#[cfg(target_os = "linux")]
fn cgroup_v1_limit() -> Option<usize> {
    let raw = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok()?;
    let v = raw.trim().parse::<usize>().ok()?;
    // v1 spells "unlimited" as a value near usize::MAX rather than a word.
    // Anything at or above a petabyte is that sentinel, not a real limit.
    (v < (1 << 50)).then_some(v)
}

#[cfg(target_os = "linux")]
fn mem_total() -> Option<usize> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    // "MemTotal:       16316360 kB"
    let kb: usize = line.split_whitespace().nth(1)?.parse().ok()?;
    kb.checked_mul(1024)
}

/// Resolve `fraction` of the host's memory limit into a byte ceiling.
/// `None` when the limit is unknown, so the caller can fall back to its own
/// default rather than silently running unbounded.
pub fn memory_fraction_bytes(fraction: f64) -> Option<usize> {
    // NaN included deliberately: it is not a request for anything.
    if fraction.is_nan() || fraction <= 0.0 {
        return None;
    }
    let limit = memory_limit_bytes()?;
    // Clamped so a typo like `10.0` can't ask for ten times the box.
    let f = fraction.min(1.0);
    Some(((limit as f64) * f) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn available_cpus_is_at_least_one() {
        assert!(available_cpus() >= 1);
    }

    #[test]
    fn a_zero_or_negative_fraction_is_not_a_request() {
        assert_eq!(memory_fraction_bytes(0.0), None);
        assert_eq!(memory_fraction_bytes(-1.0), None);
        assert_eq!(memory_fraction_bytes(f64::NAN), None);
    }

    #[test]
    fn a_fraction_resolves_to_a_share_of_the_limit_when_known() {
        // Only assert the relationship, not a value: CI hosts differ, and the
        // whole point is that this reads the host.
        if let Some(limit) = memory_limit_bytes() {
            assert!(limit > 0);
            let half = memory_fraction_bytes(0.5).expect("known limit yields a value");
            assert!(half <= limit, "{half} should not exceed {limit}");
            // An over-1.0 fraction is clamped rather than trusted.
            assert_eq!(memory_fraction_bytes(10.0), Some(limit));
        }
    }
}
