use std::sync::Arc;

use arrow_csv2::clickbench;
use arrow_csv2::{ArrowCsv2Decoder, ParallelCsvSource};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::source::DataSourceExec;
use object_store::path::Path as ObjectPath;

const FILE: &str = "hits_100mb.csv";
const BATCH_SIZE: usize = 8192;

fn main() {
    let schema = clickbench::schema();
    let file_len = std::fs::metadata(FILE)
        .expect("hits_100mb.csv not found")
        .len() as usize;

    let num_partitions = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    if num_partitions == 0 {
        let file = std::fs::File::open(FILE).unwrap();
        let reader = arrow_csv2::ReaderBuilder::new(schema)
            .with_batch_size(BATCH_SIZE)
            .build(file);

        let _ = reader.collect::<Vec<_>>();
    } else {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let plan = build_parallel_csv(&schema, file_len, num_partitions);
            let ctx = SessionContext::new();
            let _ = collect(plan, ctx.task_ctx()).await.unwrap();
        })
    }
}

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
        ArrowCsv2Decoder,
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
