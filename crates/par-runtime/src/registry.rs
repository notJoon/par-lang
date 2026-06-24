use crate::flat::runtime::ExternalFn;
use crate::linker::{Linked, Unlinked};
use crate::pkgid::BuiltinPackage;
use std::collections::HashMap;
use std::sync::LazyLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PackageRef<'a> {
    Builtin(BuiltinPackage),
    Special(&'a str),
    Local(&'a str),
    Remote(&'a str),
}

impl PackageRef<'_> {
    pub const CORE: Self = Self::Builtin(BuiltinPackage::Core);
    pub const BASIC: Self = Self::Builtin(BuiltinPackage::Basic);
    pub const MPSC: Self = Self::Builtin(BuiltinPackage::Mpsc);
}

#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
pub struct DefinitionRef<'a> {
    pub package: PackageRef<'a>,
    pub path: &'a [&'a str],
    pub module: &'a str,
    pub name: &'a str,
}

#[derive(Clone, Copy)]
pub struct ExternalDef {
    pub path: DefinitionRef<'static>,
    pub f: ExternalFn,
}

inventory::collect!(ExternalDef);

type Registry = HashMap<Unlinked, Linked>;

static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    inventory::iter::<ExternalDef>
        .into_iter()
        .map(|&ExternalDef { path, f }| (path.into(), f))
        .collect()
});

pub fn get_external_fn(path: &Unlinked) -> Option<ExternalFn> {
    REGISTRY.get(path).copied()
}
