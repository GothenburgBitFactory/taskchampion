use pretty_assertions::assert_eq;
use proptest::prelude::*;
use taskchampion::{Operations, Replica, ServerConfig, StorageConfig, TaskData, Uuid};
use tempfile::TempDir;

#[derive(Debug, Clone)]
enum Action {
    Create,
    Update(String, String),
    Delete,
    Sync,
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        Just(Action::Create),
        ("(description|project|due)", "(a|b|c)").prop_map(|(p, v)| Action::Update(p, v)),
        Just(Action::Delete),
        Just(Action::Sync),
    ]
}

fn actions() -> impl Strategy<Value = Vec<(Action, u8)>> {
    proptest::collection::vec((action(), (0..3u8)), 0..100)
}

proptest! {
#[test]
/// Check that various sequences of operations on mulitple db's do not get the db's into an
/// incompatible state.  The main concern here is that there might be a sequence of operations
/// that results in a task being in different states in different replicas. Different tasks
/// cannot interfere with one another, so this focuses on a single task.
fn multi_replica_sync(action_sequence in actions()) {
    let tmp_dir = TempDir::new().expect("TempDir failed");
    let uuid = Uuid::parse_str("83a2f9ef-f455-4195-b92e-a54c161eebfc").unwrap();
    let server_config = ServerConfig::Local {
        server_dir: tmp_dir.path().to_path_buf(),
    };
    let mut server = server_config.into_server()?;
    let mut replicas = [
        Replica::new(StorageConfig::InMemory.into_storage().unwrap()),
        Replica::new(StorageConfig::InMemory.into_storage().unwrap()),
        Replica::new(StorageConfig::InMemory.into_storage().unwrap()),
    ];

    for (action, rep) in action_sequence {
        println!("{:?} on rep {}", action, rep);

        let rep = &mut replicas[rep as usize];
        match action {
            Action::Create => {
                if rep.get_task_data(uuid).unwrap().is_none() {
                    let mut ops = Operations::new();
                    TaskData::create(uuid, &mut ops);
                    rep.commit_operations(ops).unwrap();
                }
            },
            Action::Update(p, v) => {
                if let Some(mut t) = rep.get_task_data(uuid).unwrap() {
                    let mut ops = Operations::new();
                    t.update(p, Some(v), &mut ops);
                    rep.commit_operations(ops).unwrap();
                }
            },
            Action::Delete => {
                if let Some(mut t) = rep.get_task_data(uuid).unwrap() {
                    let mut ops = Operations::new();
                    t.delete(&mut ops);
                    rep.commit_operations(ops).unwrap();
                }
            },
            Action::Sync => rep.sync(&mut server, false).unwrap(),
        }
    }

    // Sync all of the replicas, twice, to flush out any un-synced changes.
    for rep in &mut replicas {
        rep.sync(&mut server, false).unwrap()
    }
    for rep in &mut replicas {
        rep.sync(&mut server, false).unwrap()
    }

    let t0 = replicas[0].get_task_data(uuid).unwrap();
    let t1 = replicas[1].get_task_data(uuid).unwrap();
    let t2 = replicas[2].get_task_data(uuid).unwrap();
    assert_eq!(t0, t1);
    assert_eq!(t1, t2);
}
}
