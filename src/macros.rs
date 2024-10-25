//! Helper macros.

/// Implements `TryFrom` for enums from their numerical representation.
macro_rules! numerical_enum {
    (
        $(#[$attr:meta])*
        $vis:vis enum $enum_name:ident as $repr:tt {
            $(
                $(#[$id_attr:meta])*
                $identifier:ident = $value:expr,
            )+
        }
    ) => {
        $(#[$attr])*
        #[derive(Copy, Clone, Debug, Eq, PartialEq)]
        #[repr($repr)]
        $vis enum $enum_name {
            $(
                $(#[$id_attr])*
                $identifier = $value,
            )+
        }

        impl TryFrom<$repr> for $enum_name {
            type Error = std::io::Error;

            fn try_from(val: $repr) -> std::io::Result<Self> {
                match val {
                    $(x if x == $value => Ok($enum_name::$identifier),)*
                    _ => Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Invalid value for {}: {:x}",
                                stringify!($enum_name),
                                val,
                            ),
                    )),
                }
            }
        }
    }
}

pub(crate) use numerical_enum;
