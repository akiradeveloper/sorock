use tonic_reflection::server::{ServerReflection, ServerReflectionServer};

const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/lolraft_descriptor.bin");

/// Create a reflection service.
pub fn new() -> ServerReflectionServer<impl ServerReflection> {
    let svc = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    svc
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_new() {
        new();
    }
}
