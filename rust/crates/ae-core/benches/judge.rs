use ae_core::judge::{judge_output, normalize_output};
use ae_core::model::Checker;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench(c: &mut Criterion) {
    let lines = "Hello World 42  \n".repeat(50);
    let tok = "12345 67890 hello world 3.14159\n".repeat(100);
    let flt = "3.14159265 2.71828182 1.41421356 1.7320508\n".repeat(100);

    c.bench_function("normalize_output", |b| {
        b.iter(|| normalize_output(black_box(lines.as_bytes())));
    });
    c.bench_function("judge_exact", |b| {
        b.iter(|| {
            judge_output(
                Checker::Exact,
                black_box(lines.as_bytes()),
                black_box(lines.as_bytes()),
            )
        });
    });
    c.bench_function("judge_token", |b| {
        b.iter(|| {
            judge_output(
                Checker::Token,
                black_box(tok.as_bytes()),
                black_box(tok.as_bytes()),
            )
        });
    });
    c.bench_function("judge_float", |b| {
        b.iter(|| {
            judge_output(
                Checker::Float,
                black_box(flt.as_bytes()),
                black_box(flt.as_bytes()),
            )
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
