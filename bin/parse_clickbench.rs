use std::sync::Arc;
use std::time::Instant;

use arrow_csv2::{ParallelCsvSource, clickbench};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::source::DataSourceExec;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;

const BATCH_SIZE: usize = 8192;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();

    let file = args.get(1).map(|s| s.as_str()).unwrap_or("hits.csv");
    let file_len = std::fs::metadata(file)
        .unwrap_or_else(|_| panic!("{file} not found"))
        .len() as usize;

    let partition_size = arg(&args, "--partition-size=")
        .and_then(parse_size)
        .unwrap_or(64 << 20);
    let concurrency: usize = arg(&args, "--concurrency=")
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let num_partitions = file_len.div_ceil(partition_size);

    eprintln!(
        "file: {file}, partitions: {num_partitions}, size: {}mb, concurrency: {concurrency}",
        partition_size >> 20
    );

    let start = Instant::now();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let plan = build_plan(file, file_len, num_partitions, concurrency);
        let ctx = SessionContext::new();
        let num_parts = plan.properties().output_partitioning().partition_count();

        let mut handles = Vec::new();

        for i in 0..num_parts {
            let stream = plan.execute(i, ctx.task_ctx()).unwrap();
            handles.push(tokio::spawn(async move {
                let mut stream = stream;
                while let Some(batch) = stream.next().await {
                    batch.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    });

    let gb = file_len as f64 / (1024.0 * 1024.0 * 1024.0);
    eprintln!(
        "time: {:.3}s ({:.2} gb/s)",
        start.elapsed().as_secs_f64(),
        gb / start.elapsed().as_secs_f64()
    );
}

fn build_plan(
    file: &str,
    file_len: usize,
    num_partitions: usize,
    concurrency: usize,
) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    let schema = clickbench::schema();
    let chunk_size = file_len / num_partitions;
    let boundaries: Arc<[usize]> = (0..num_partitions)
        .map(|i| i * chunk_size)
        .chain(std::iter::once(file_len))
        .collect::<Vec<_>>()
        .into();

    let abs_path = std::fs::canonicalize(file).unwrap();
    let object_path = ObjectPath::from_absolute_path(&abs_path).unwrap();

    let source = Arc::new(ParallelCsvSource::with_concurrency(
        schema,
        object_path,
        boundaries.clone(),
        BATCH_SIZE,
        Some(concurrency),
    ));

    let url = ObjectStoreUrl::parse("file://").unwrap();
    let mut builder = FileScanConfigBuilder::new(url, source);
    for i in 0..num_partitions {
        let pf = PartitionedFile::new(file.to_string(), file_len as u64)
            .with_range(boundaries[i] as i64, boundaries[i + 1] as i64);
        builder = builder.with_file_group(FileGroup::new(vec![pf]));
    }

    DataSourceExec::from_data_source(builder.build())
}

fn arg<'a>(args: &'a [String], prefix: &str) -> Option<&'a str> {
    args.iter().find_map(|s| s.strip_prefix(prefix))
}

fn parse_size(s: &str) -> Option<usize> {
    let s = s.trim();

    if let Some(n) = s.strip_suffix("mb").or(s.strip_suffix("MB")) {
        return n.trim().parse::<usize>().ok().map(|n| n << 20);
    }

    if let Some(n) = s.strip_suffix("gb").or(s.strip_suffix("GB")) {
        return n.trim().parse::<usize>().ok().map(|n| n << 30);
    }

    s.parse().ok()
}
