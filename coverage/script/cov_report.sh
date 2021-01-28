LOL_ROOT=..
OUTPUT=/tmp/merged.profdata

llvm-profdata-11 merge ${LOL_ROOT}/integration-tests/cov/*.profraw -o ${OUTPUT}
llvm-cov-11 report ${LOL_ROOT}/target/debug/kvs-server \
    -Xdemangler=rustfilt \
    -instr-profile=${OUTPUT} \
    --ignore-filename-regex="(.cargo|rustc)" 