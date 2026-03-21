use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::current_time_ms;

    #[test]
    fn current_time_ms_is_non_decreasing_across_calls() {
        let t1 = current_time_ms();
        let t2 = current_time_ms();
        assert!(t2 >= t1);
    }
}
