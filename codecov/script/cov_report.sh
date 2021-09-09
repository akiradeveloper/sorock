MERGED=/tmp/merged.profdata

llvm-profdata merge ../integration-tests/cov/*.profraw -o ${MERGED}
llvm-cov report /tmp/cargo-target/debug/kvs-server \
    -Xdemangler=rustfilt \
    -instr-profile=${MERGED} \
    --ignore-filename-regex="(.cargo|rustc)" 