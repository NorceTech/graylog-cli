use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::json;

fn bench_json_serialize(c: &mut Criterion) {
    let messages: Vec<serde_json::Map<String, serde_json::Value>> = (0..100)
        .map(|i| {
            let mut map = serde_json::Map::new();
            map.insert("message".to_string(), json!(format!("test message {i}")));
            map.insert("level".to_string(), json!("ERROR"));
            map.insert("timestamp".to_string(), json!("2026-01-15T14:30:00Z"));
            map
        })
        .collect();

    c.bench_function("serialize_100_messages", |b| {
        b.iter(|| serde_json::to_string(black_box(&messages)).unwrap())
    });
}

fn bench_json_deserialize(c: &mut Criterion) {
    let messages: Vec<serde_json::Map<String, serde_json::Value>> = (0..100)
        .map(|i| {
            let mut map = serde_json::Map::new();
            map.insert("message".to_string(), json!(format!("test message {i}")));
            map.insert("level".to_string(), json!("ERROR"));
            map.insert("timestamp".to_string(), json!("2026-01-15T14:30:00Z"));
            map
        })
        .collect();
    let serialized = serde_json::to_string(&messages).unwrap();

    c.bench_function("deserialize_100_messages", |b| {
        b.iter(|| {
            let _: Vec<serde_json::Map<String, serde_json::Value>> =
                serde_json::from_str(black_box(&serialized)).unwrap();
        })
    });
}

criterion_group!(benches, bench_json_serialize, bench_json_deserialize);
criterion_main!(benches);
