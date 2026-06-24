use std::fmt;

use arcstr::ArcStr;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PackageId {
    Builtin(BuiltinPackage),
    Special(ArcStr),
    Local(ArcStr),
    Remote(ArcStr),
}

impl PackageId {
    pub fn name(&self) -> &str {
        match self {
            PackageId::Builtin(name) => name.as_str(),
            PackageId::Special(name) | PackageId::Local(name) | PackageId::Remote(name) => {
                name.as_str()
            }
        }
    }

    pub fn is_regular(&self) -> bool {
        matches!(self, Self::Local(_) | Self::Remote(_))
    }

    pub fn local_path(&self) -> Option<&str> {
        match self {
            PackageId::Local(name) => Some(name),
            _ => None,
        }
    }

    pub fn remote_path(&self) -> Option<&str> {
        match self {
            PackageId::Remote(name) => Some(name),
            _ => None,
        }
    }
}

impl fmt::Display for PackageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", self.name())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BuiltinPackage {
    Core,
    Basic,
    Mpsc,
}

impl BuiltinPackage {
    pub const ALL: &[Self] = &[Self::Core, Self::Basic, Self::Mpsc];

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "core" => Some(BuiltinPackage::Core),
            "basic" => Some(BuiltinPackage::Basic),
            "mpsc" => Some(BuiltinPackage::Mpsc),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            BuiltinPackage::Core => "core",
            BuiltinPackage::Basic => "basic",
            BuiltinPackage::Mpsc => "mpsc",
        }
    }
}

impl fmt::Display for BuiltinPackage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
