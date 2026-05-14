pub fn matches_pattern(s: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    glob_match(s.as_bytes(), pattern.as_bytes())
}

pub fn glob_match(s: &[u8], p: &[u8]) -> bool {
    match (s.first(), p.first()) {
        (_, Some(b'*')) => glob_match(s, &p[1..]) || (!s.is_empty() && glob_match(&s[1..], p)),
        (Some(sc), Some(pc)) => (*pc == b'?' || sc == pc) && glob_match(&s[1..], &p[1..]),
        (None, None) => true,
        _ => false,
    }
}
