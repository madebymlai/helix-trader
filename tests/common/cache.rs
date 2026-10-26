//! The live tests' records in the node's cache in Redis (`live::redis`):
//! each test runs its nodes under a trader of its own, whose keys Nautilus
//! prefixes with `trader-<trader id>:`, and deletes them at the end.

use redis::AsyncCommands;

/// Deletes every key of `trader_id` in the node's Redis; returns how many.
///
/// # Panics
///
/// Panics if Redis on 127.0.0.1:6379 cannot be reached.
pub async fn delete_trader(trader_id: &str) -> usize {
    let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let mut keys = Vec::new();
    let mut scan = connection
        .scan_match::<_, String>(format!("trader-{trader_id}:*"))
        .await
        .unwrap();
    while let Some(key) = scan.next_item().await {
        keys.push(key.unwrap());
    }
    drop(scan);
    if !keys.is_empty() {
        let _: () = connection.del(&keys).await.unwrap();
    }
    keys.len()
}
