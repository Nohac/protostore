use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, str::FromStr};

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Hash32(pub [u8; 32]);

impl Hash32 {
    pub fn digest(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    pub fn zero() -> Self {
        Self([0; 32])
    }

    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Display for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl FromStr for Hash32 {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let bytes = hex::decode(value).with_context(|| format!("decoding hash {value}"))?;
        if bytes.len() != 32 {
            bail!("expected 32-byte hex hash, got {} bytes", bytes.len());
        }
        let mut out = [0; 32];
        out.copy_from_slice(&bytes);
        Ok(Self(out))
    }
}

impl Serialize for Hash32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Hash32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
        )]
        pub struct $name(pub Hash32);

        impl $name {
            pub fn new(hash: Hash32) -> Self {
                Self(hash)
            }

            pub fn zero() -> Self {
                Self(Hash32::zero())
            }

            pub fn to_hex(self) -> String {
                self.0.to_hex()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.to_hex())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.to_hex())
            }
        }

        impl FromStr for $name {
            type Err = anyhow::Error;

            fn from_str(value: &str) -> Result<Self> {
                Ok(Self(Hash32::from_str(value)?))
            }
        }
    };
}

id_type!(TreeId);
id_type!(LayoutId);
id_type!(PackId);
id_type!(ChunkId);
id_type!(ProfileId);
