use std::str::FromStr;

macro_rules! impl_hook_kind {
    ($self:ident, $name:expr, $latest_version:expr) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $self;

        impl private::Sealed for $self {}

        impl HookKind for $self {
            const NAME: &str = $name;
            const VERSION: &str = $latest_version;
            const NAME_VERSION: &str = concat!($name, "_", $latest_version);
        }

        impl FromStr for $self {
            type Err = decode::Error;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                if input == Self::NAME {
                    Ok(Self)
                } else {
                    Err(Self::Err::Unknown(input.to_owned()))
                }
            }
        }
    };
}

pub trait HookKind:
    private::Sealed + std::fmt::Debug + Copy + Clone + FromStr<Err = decode::Error>
{
    /// The name of this hook kind
    ///
    /// This is the string users put in their hook path.
    const NAME: &'static str;

    /// The current version of this hook kind.
    const VERSION: &'static str;

    /// The concatenated name and version.
    ///
    /// This is the actual string users should put in the first hook path component.
    /// It is here, because we already know both values at compile time, and as such can
    /// construct this as a convenience.
    const NAME_VERSION: &'static str;
}

mod private {
    // The whole point of this trait is being a unnameable type.
    #[allow(unnameable_types)]
    pub trait Sealed {}
}

#[allow(missing_docs)]
pub mod decode {
    #[derive(Debug, thiserror::Error)]
    pub enum Error {
        #[error("Unknown hook type: '{0}'")]
        Unknown(String),
    }
}

impl_hook_kind! {OnAdd, "on-add", "v3"}
impl_hook_kind! {OnModify, "on-modify", "v3"}
impl_hook_kind! {OnLaunch, "on-launch", "v3"}
impl_hook_kind! {OnExit, "on-exit", "v3"}
impl_hook_kind! {OnCommit, "on-commit", "v1"}
