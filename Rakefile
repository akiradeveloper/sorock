desc "Build docs.rs and serve on http://0.0.0.0:3000"
task :doc do
    sh "cargo doc"
    sh "python3 -m http.server --directory target/doc 3000"
end

desc "Build book and serve on http://0.0.0.0:3000"
task :book do
    sh "mdbook serve book -p 3000 -n 0.0.0.0"
end

desc "Run all tests"
task :test do
    sh "cargo test -- --test-threads=1"
end