use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};

fn clickbench_schema() -> Arc<Schema> {
    use DataType::*;
    let f = |name: &str, dt: DataType| {
        let nullable = matches!(dt, Utf8);
        Field::new(name, dt, nullable)
    };
    Arc::new(Schema::new(vec![
        f("WatchID", Int64),
        f("JavaEnable", Int16),
        f("Title", Utf8),
        f("GoodEvent", Int16),
        f("EventTime", Utf8),
        f("EventDate", Utf8),
        f("CounterID", Int32),
        f("ClientIP", Int32),
        f("RegionID", Int32),
        f("UserID", Int64),
        f("CounterClass", Int16),
        f("OS", Int16),
        f("UserAgent", Int16),
        f("URL", Utf8),
        f("Referer", Utf8),
        f("IsRefresh", Int16),
        f("RefererCategoryID", Int16),
        f("RefererRegionID", Int32),
        f("URLCategoryID", Int16),
        f("URLRegionID", Int32),
        f("ResolutionWidth", Int16),
        f("ResolutionHeight", Int16),
        f("ResolutionDepth", Int16),
        f("FlashMajor", Int16),
        f("FlashMinor", Int16),
        f("FlashMinor2", Utf8),
        f("NetMajor", Int16),
        f("NetMinor", Int16),
        f("UserAgentMajor", Int16),
        f("UserAgentMinor", Utf8),
        f("CookieEnable", Int16),
        f("JavascriptEnable", Int16),
        f("IsMobile", Int16),
        f("MobilePhone", Int16),
        f("MobilePhoneModel", Utf8),
        f("Params", Utf8),
        f("IPNetworkID", Int32),
        f("TraficSourceID", Int16),
        f("SearchEngineID", Int16),
        f("SearchPhrase", Utf8),
        f("AdvEngineID", Int16),
        f("IsArtifical", Int16),
        f("WindowClientWidth", Int16),
        f("WindowClientHeight", Int16),
        f("ClientTimeZone", Int16),
        f("ClientEventTime", Utf8),
        f("SilverlightVersion1", Int16),
        f("SilverlightVersion2", Int16),
        f("SilverlightVersion3", Int32),
        f("SilverlightVersion4", Int16),
        f("PageCharset", Utf8),
        f("CodeVersion", Int32),
        f("IsLink", Int16),
        f("IsDownload", Int16),
        f("IsNotBounce", Int16),
        f("FUniqID", Int64),
        f("OriginalURL", Utf8),
        f("HID", Int32),
        f("IsOldCounter", Int16),
        f("IsEvent", Int16),
        f("IsParameter", Int16),
        f("DontCountHits", Int16),
        f("WithHash", Int16),
        f("HitColor", Utf8),
        f("LocalEventTime", Utf8),
        f("Age", Int16),
        f("Sex", Int16),
        f("Income", Int16),
        f("Interests", Int16),
        f("Robotness", Int16),
        f("RemoteIP", Int32),
        f("WindowName", Int32),
        f("OpenerName", Int32),
        f("HistoryLength", Int16),
        f("BrowserLanguage", Utf8),
        f("BrowserCountry", Utf8),
        f("SocialNetwork", Utf8),
        f("SocialAction", Utf8),
        f("HTTPError", Int16),
        f("SendTiming", Int32),
        f("DNSTiming", Int32),
        f("ConnectTiming", Int32),
        f("ResponseStartTiming", Int32),
        f("ResponseEndTiming", Int32),
        f("FetchTiming", Int32),
        f("SocialSourceNetworkID", Int16),
        f("SocialSourcePage", Utf8),
        f("ParamPrice", Int64),
        f("ParamOrderID", Utf8),
        f("ParamCurrency", Utf8),
        f("ParamCurrencyID", Int16),
        f("OpenstatServiceName", Utf8),
        f("OpenstatCampaignID", Utf8),
        f("OpenstatAdID", Utf8),
        f("OpenstatSourceID", Utf8),
        f("UTMSource", Utf8),
        f("UTMMedium", Utf8),
        f("UTMCampaign", Utf8),
        f("UTMContent", Utf8),
        f("UTMTerm", Utf8),
        f("FromTag", Utf8),
        f("HasGCLID", Int16),
        f("RefererHash", Int64),
        f("URLHash", Int64),
        f("CLID", Int32),
    ]))
}

fn bench_clickbench(c: &mut Criterion) {
    let raw = std::fs::read("hits_100mb.csv")
        .expect("hits_100mb.csv not found, run: cargo run --release --bin slice_clickbench");

    let schema = clickbench_schema();

    c.bench_function("arrow-csv2::Decoder (clickbench 100MB)", |b| {
        b.iter(|| {
            let mut decoder = arrow_csv2::ReaderBuilder::new(schema.clone())
                .with_batch_size(8192)
                .build_decoder();

            let mut offset = 0;
            let mut batches = Vec::new();
            loop {
                let consumed = decoder.decode(&raw[offset..]).unwrap();
                offset += consumed;
                if consumed == 0 || decoder.capacity() == 0 {
                    if let Some(batch) = decoder.flush().unwrap() {
                        batches.push(batch);
                    }
                    if consumed == 0 && decoder.capacity() > 0 {
                        break;
                    }
                }
            }
            batches
        });
    });

    c.bench_function("arrow-csv2::Reader (clickbench 100MB)", |b| {
        b.iter(|| {
            let reader = arrow_csv2::ReaderBuilder::new(schema.clone())
                .with_batch_size(8192)
                .build_buffered(raw.as_slice());

            reader.collect::<Result<Vec<_>, _>>().unwrap()
        });
    });

    c.bench_function("arrow-csv::Reader (clickbench 100MB)", |b| {
        b.iter(|| {
            let reader = arrow_csv::ReaderBuilder::new(schema.clone())
                .with_batch_size(8192)
                .build_buffered(raw.as_slice())
                .unwrap();

            reader.collect::<Result<Vec<_>, _>>().unwrap()
        });
    });
}

criterion_group!(benches, bench_clickbench);
criterion_main!(benches);
