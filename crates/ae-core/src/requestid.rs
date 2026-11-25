/// Returns a fresh request ID: a UUIDv7 in canonical hyphenated form, as the Go gateway generates.
#[must_use]
pub fn new_request_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unique_v7_uuids() {
        let a = new_request_id();
        let b = new_request_id();
        assert_ne!(a, b);
        let u = uuid::Uuid::parse_str(&a).unwrap();
        assert_eq!(u.get_version_num(), 7);
    }
}
