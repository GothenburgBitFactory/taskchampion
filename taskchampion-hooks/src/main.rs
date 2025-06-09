use std::path::PathBuf;

use taskchampion::{Replica, StorageConfig, Uuid};
use tw_hooks::{
    hook::hook_kinds::{OnAdd, OnModify},
    hooks,
};

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

    let mut ops = vec![];
    let base_task = replica.create_task(Uuid::new_v4(), &mut ops).unwrap();
    let mut new_task = base_task.clone();
    new_task
        .set_description("Test".to_owned(), &mut ops)
        .expect("to work, as hardcoded");
    drop(ops);

    hooks::<OnAdd>(PathBuf::from("./hook_structure"))
        .unwrap()
        .for_each(|hook| {
            let hook = hook.unwrap().unwrap();
            println!("Calling Hook: {}", hook);
            dbg!(
                hook.execute(base_task.clone())
                    .map_err(|err| panic!("{}", err))
                    .unwrap()
            );
        });

    hooks::<OnModify>(PathBuf::from("./hook_structure"))
        .unwrap()
        .for_each(|hook| {
            let hook = hook.unwrap().unwrap();
            println!("Calling Hook: {}", hook);
            dbg!(
                hook.execute(base_task.clone(), new_task.clone())
                    .map_err(|err| panic!("{}", err))
                    .unwrap()
            );
        });
}
