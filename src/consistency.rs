pub struct check_result {
    pub result: bool,
    pub latency: i64
}

pub async fn do_consistency_check() -> Result<check_result, ()> {
    Ok(check_result {
        result: true,
        latency: 0
    })
}
