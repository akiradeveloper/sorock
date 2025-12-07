task :doc do
    sh "cargo doc"
    sh "python3 -m http.server --directory target/doc 3000"
end