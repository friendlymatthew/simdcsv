use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::source::DataSourceExec;
use duckdb::{Config, Connection};
use object_store::path::Path as ObjectPath;
use tokio::runtime::Runtime;

use arrow_csv2::ParallelCsvSource;
use arrow_csv2::clickbench;

const FILE: &str = "hits_100mb.csv";
const BATCH_SIZE: usize = 8192;

fn build_parallel_csv(
    schema: &arrow_schema::SchemaRef,
    file_len: usize,
    num_partitions: usize,
) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    let chunk_size = file_len / num_partitions;
    let boundaries: Arc<[usize]> = (0..num_partitions)
        .map(|i| i * chunk_size)
        .chain(std::iter::once(file_len))
        .collect::<Vec<_>>()
        .into();

    let abs_path = std::fs::canonicalize(FILE).unwrap();
    let object_path = ObjectPath::from_absolute_path(&abs_path).unwrap();

    let source = Arc::new(ParallelCsvSource::new(
        schema.clone(),
        object_path,
        boundaries.clone(),
        BATCH_SIZE,
    ));

    let url = ObjectStoreUrl::parse("file://").unwrap();
    let mut builder = FileScanConfigBuilder::new(url, source);
    for i in 0..num_partitions {
        let file = PartitionedFile::new(FILE.to_string(), file_len as u64)
            .with_range(boundaries[i] as i64, boundaries[i + 1] as i64);
        builder = builder.with_file_group(FileGroup::new(vec![file]));
    }
    DataSourceExec::from_data_source(builder.build())
}

fn bench_parallel_csv(c: &mut Criterion) {
    let file_len = std::fs::metadata(FILE)
        .expect("hits_100mb.csv not found")
        .len() as usize;
    let schema = clickbench::schema();
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("arrow-csv2 (clickbench 100MB)");
    group.sample_size(10);

    for num_partitions in [1, 2, 4, 8, 12, 16] {
        group.bench_function(format!("arrow-csv2 ({num_partitions}p)"), |b| {
            b.to_async(&rt).iter(|| {
                let schema = schema.clone();
                async move {
                    let plan = build_parallel_csv(&schema, file_len, num_partitions);
                    let ctx = SessionContext::new();
                    datafusion::physical_plan::collect(plan, ctx.task_ctx())
                        .await
                        .unwrap()
                }
            });
        });
    }

    group.finish();
}

fn bench_datafusion_csv(c: &mut Criterion) {
    let schema = clickbench::schema();
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("datafusion csv (clickbench 100MB)");
    group.sample_size(10);

    for num_partitions in [1, 2, 4, 8, 12, 16] {
        group.bench_function(format!("datafusion-csv ({num_partitions}p)"), |b| {
            b.to_async(&rt).iter(|| {
                let schema = schema.clone();
                async move {
                    let config = SessionConfig::new().with_target_partitions(num_partitions);
                    let ctx = SessionContext::new_with_config(config);
                    let opts = CsvReadOptions::new()
                        .has_header(false)
                        .schema(schema.as_ref())
                        .file_extension(".csv");

                    let df = ctx.read_csv(FILE, opts).await.unwrap();
                    df.collect().await.unwrap()
                }
            });
        });
    }

    // correct mode: newlines_in_values=true (forced single partition)
    group.bench_function("datafusion-csv (correct, 1p)", |b| {
        b.to_async(&rt).iter(|| {
            let schema = schema.clone();
            async move {
                let config = SessionConfig::new().with_target_partitions(16);
                let ctx = SessionContext::new_with_config(config);
                let opts = CsvReadOptions::new()
                    .has_header(false)
                    .schema(schema.as_ref())
                    .newlines_in_values(true)
                    .file_extension(".csv");

                let df = ctx.read_csv(FILE, opts).await.unwrap();
                df.collect().await.unwrap()
            }
        });
    });

    group.finish();
}

/*
duckdb's poor numbers are a bit suspect to me

from what I can gather, the benchmark closure is measuring DuckDB's csv parsing + the Arrow schema conversion
though most of the cost is definitely coming from the former
*/
fn bench_duckdb_csv(c: &mut Criterion) {
    use arrow_schema::DataType;

    let abs_path = std::fs::canonicalize(FILE).expect("hits_100mb.csv not found");
    let path_str = abs_path.to_str().unwrap();

    let schema = clickbench::schema();
    let columns: String = schema
        .fields()
        .iter()
        .map(|f| {
            let duckdb_type = match f.data_type() {
                DataType::Int16 => "SMALLINT",
                DataType::Int32 => "INTEGER",
                DataType::Int64 => "BIGINT",
                DataType::Utf8 => "VARCHAR",
                other => panic!("unmapped type: {other:?}"),
            };
            format!("'{}': '{}'", f.name(), duckdb_type)
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut group = c.benchmark_group("duckdb csv (clickbench 100MB)");
    group.sample_size(10);

    for num_threads in [1, 2, 4, 8, 12, 16] {
        let query =
            format!("SELECT * FROM read_csv('{path_str}', header=false, columns={{{columns}}})");
        let config = Config::default()
            .with("threads", num_threads.to_string())
            .unwrap();
        let conn = Connection::open_in_memory_with_flags(config).unwrap();

        group.bench_function(format!("duckdb-csv ({num_threads}t)"), |b| {
            b.iter(|| {
                // prepare outside the loop lets DuckDb cache the query plan
                // datafusion's collect rebuilds the plan every iteration
                let mut stmt = conn.prepare(&query).unwrap();
                let rows = stmt.query_arrow([]).unwrap();
                let batches = rows.collect::<Vec<_>>();
                assert!(!batches.is_empty());
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parallel_csv,
    bench_datafusion_csv,
    bench_duckdb_csv
);
criterion_main!(benches);
