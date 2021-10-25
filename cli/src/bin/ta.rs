use color_eyre::eyre::{eyre, Report, WrapErr};

fn main() -> Result<(), Report> {
    color_eyre::install()?;

    taskchampion_cli::main()?;
    Ok(())
}
