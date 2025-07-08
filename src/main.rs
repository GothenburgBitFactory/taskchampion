use taskchampion::{Operations, Replica, StorageConfig, Uuid};

fn main() {
    stderrlog::new()
        .module(module_path!())
        // .modules(&[])
        .quiet(false)
        .show_module_names(false)
        .color(stderrlog::ColorChoice::Auto)
        .verbosity(4)
        .timestamp(stderrlog::Timestamp::Off)
        .init()
        .expect("Let's just hope that this does not panic");

    let storage = StorageConfig::InMemory {}.into_storage().unwrap();
    let mut replica = Replica::new(storage);

    replica.with_hooks("./tests/hook_structure");

    let mut ops = vec![];
    let base_task = replica.create_task(Uuid::new_v4(), &mut ops).unwrap();
    let mut new_task = base_task.clone();
    new_task
        .set_description("Test".to_owned(), &mut ops)
        .expect("to work, as hardcoded");
    replica
        .commit_operations(ops)
        .map_err(|err| panic!("{}", err))
        .unwrap();

    for task in replica.all_tasks().unwrap() {
        println!("Have task: {} -> {}", task.0, task.1.get_description());
    }

    let mut found = replica.get_task(new_task.get_uuid()).unwrap().unwrap();

    let mut ops = Operations::new();
    found
        .set_description(
            found.get_description().to_owned() + " (extended by rust)",
            &mut ops,
        )
        .unwrap();

    replica
        .commit_operations(ops)
        .map_err(|err| panic!("{}", err))
        .unwrap();
}
