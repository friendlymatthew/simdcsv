use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::display::ArrayFormatter;
use arrow_csv2::{ParallelCsvSource, ReaderBuilder, clickbench};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::source::DataSourceExec;
use object_store::path::Path as ObjectPath;

fn taxi_zone_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("LocationID", DataType::Utf8, true),
        Field::new("Borough", DataType::Utf8, true),
        Field::new("Zone", DataType::Utf8, true),
        Field::new("service_zone", DataType::Utf8, true),
    ]))
}

#[test]
fn taxi_zone_matches_arrow_csv() {
    let raw = std::fs::read("taxi_zone_lookup.csv").expect("missing csv");
    let schema = taxi_zone_schema();

    let ours = ReaderBuilder::new(schema.clone())
        .with_header(true)
        .with_batch_size(8192)
        .build(raw.as_slice())
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    let theirs = arrow_csv::ReaderBuilder::new(schema)
        .with_header(true)
        .with_batch_size(8192)
        .build(raw.as_slice())
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(ours.len(), theirs.len());

    for (a, b) in ours.iter().zip(&theirs) {
        assert_eq!(a, b);
    }
}

#[test]
fn clickbench_matches_arrow_csv() {
    let raw = std::fs::read("hits_100mb.csv").unwrap();
    let schema = arrow_csv2::clickbench::schema();

    let ours = ReaderBuilder::new(schema.clone())
        .with_batch_size(8192)
        .build(raw.as_slice())
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    let theirs = arrow_csv::ReaderBuilder::new(schema)
        .with_batch_size(8192)
        .build(raw.as_slice())
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(ours.len(), theirs.len());

    for (a, b) in ours.iter().zip(&theirs) {
        assert_eq!(a.num_rows(), b.num_rows());
        assert_eq!(a, b);
    }
}

const BATCH_SIZE: usize = 8192;

fn collect_parallel_csv(
    file: &str,
    schema: &SchemaRef,
    file_len: usize,
    num_partitions: usize,
    max_concurrency: Option<usize>,
) -> Vec<RecordBatch> {
    let chunk_size = file_len / num_partitions;
    let boundaries: Arc<[usize]> = (0..num_partitions)
        .map(|i| i * chunk_size)
        .chain(std::iter::once(file_len))
        .collect::<Vec<_>>()
        .into();

    let abs_path = std::fs::canonicalize(file).unwrap();
    let object_path = ObjectPath::from_absolute_path(&abs_path).unwrap();

    let source = Arc::new(ParallelCsvSource::with_concurrency(
        schema.clone(),
        object_path,
        boundaries.clone(),
        BATCH_SIZE,
        max_concurrency,
    ));

    let url = ObjectStoreUrl::parse("file://").unwrap();
    let mut builder = FileScanConfigBuilder::new(url, source);
    for i in 0..num_partitions {
        let pf = PartitionedFile::new(file.to_string(), file_len as u64)
            .with_range(boundaries[i] as i64, boundaries[i + 1] as i64);
        builder = builder.with_file_group(FileGroup::new(vec![pf]));
    }
    let plan = DataSourceExec::from_data_source(builder.build());

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = SessionContext::new();
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap()
    })
}

fn concat_batches(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row = (0..batch.num_columns())
                .map(|col| {
                    let col = batch.column(col);

                    ArrayFormatter::try_new(col, &Default::default())
                        .unwrap()
                        .value(row_idx)
                        .to_string()
                })
                .collect::<Vec<_>>();

            rows.push(row);
        }
    }
    rows
}

fn arrow_csv_baseline(file: &str, schema: &SchemaRef) -> Vec<Vec<String>> {
    let raw = std::fs::read(file).unwrap();
    let reader = arrow_csv::ReaderBuilder::new(schema.clone())
        .with_batch_size(BATCH_SIZE)
        .build(raw.as_slice())
        .unwrap();

    let batches = reader.map(|r| r.unwrap()).collect::<Vec<_>>();

    let mut rows = concat_batches(&batches);
    rows.sort();

    rows
}

#[test]
fn parallel_csv_matches_arrow_csv() {
    let file = "hits_100mb.csv";
    let file_len = std::fs::metadata(file).unwrap().len() as usize;
    let schema = clickbench::schema();
    let expected = arrow_csv_baseline(file, &schema);

    for num_partitions in [1, 2, 4, 8, 12, 16] {
        let batches = collect_parallel_csv(file, &schema, file_len, num_partitions, None);
        let mut actual = concat_batches(&batches);
        actual.sort();

        assert_eq!(expected.len(), actual.len(),);

        for (exp, act) in expected.iter().zip(&actual) {
            assert_eq!(exp, act);
        }
    }

    let partition_size = 4 * 1024 * 1024;
    let num_partitions = file_len.div_ceil(partition_size);
    let batches = collect_parallel_csv(file, &schema, file_len, num_partitions, Some(4));
    let mut actual = concat_batches(&batches);
    actual.sort();

    assert_eq!(expected.len(), actual.len(),);

    for (exp, act) in expected.iter().zip(&actual) {
        assert_eq!(exp, act,);
    }
}
