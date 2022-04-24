#![deny(clippy::all)]

use taskchampion_sync_server::storage::SqliteStorage;
use taskchampion_sync_server::{Server, ServerConfig};

#[derive(Debug, Clone)]
struct Opts {
    port: u32,
    data_dir: String,
    snapshot_versions: u32,
    snapshot_days: i64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let defaults = ServerConfig::default();

    let port = bpaf::short('p')
        .long("port")
        .help("Port on which to serve")
        .argument("PORT")
        .from_str()
        .fallback(8080);
    let data_dir = bpaf::short('d')
        .long("data-dir")
        .help("Directory in which to store data")
        .argument("PATH")
        .fallback("/var/lib/taskchampion-sync-server".into());
    let snapshot_versions = bpaf::long("snapshot-versions")
        .help("Target number of versions between snapshots")
        .argument("NUM")
        .from_str()
        .fallback(defaults.snapshot_versions);
    let snapshot_days = bpaf::long("snapshot-days")
        .help("Target number of days between snapshots")
        .argument("NUM")
        .from_str()
        .fallback(defaults.snapshot_days);

    use bpaf::construct;
    let parser = construct!(Opts {
        port,
        data_dir,
        snapshot_versions,
        snapshot_days
    });
    let opts = bpaf::Info::default()
        .version(env!("CARGO_PKG_VERSION"))
        .descr("Server for TaskChampion")
        .for_parser(parser)
        .run();

    let config = ServerConfig {
        snapshot_days: opts.snapshot_days,
        snapshot_versions: opts.snapshot_versions,
    };
    let server = Server::new(config, Box::new(SqliteStorage::new(opts.data_dir)?));

    log::warn!("Serving on port {}", opts.port);
    axum::Server::bind(&format!("0.0.0.0:{}", opts.port).parse()?)
        .serve(server.router().into_make_service())
        .await?;
    Ok(())
}

#[cfg(testFIXME)]
mod test {
    use super::*;
    use taskchampion_sync_server::storage::InMemoryStorage;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn test_index_get() {
        let server = Server::new(Default::default(), Box::new(InMemoryStorage::new()));
        let app = server.router();

        let resp = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
                     
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }
}
