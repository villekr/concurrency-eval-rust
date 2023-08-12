# Concurrency Evaluation - Rust
Rust code for [How Do You Like Your Lambda Concurrency](https://medium.com/@ville-karkkainen/how-do-you-like-your-lambda-concurrency-part-i-introduction-7a3f7ecfe4b5)-blog series.

# Requirements
* Rust 1.70.0
* cargo-lambda
* clippy

# Build Deployment Package

```
cargo lambda build --arm64 --release
zip -j ./target/lambda/concurrency_eval/bootstrap.zip ./target/lambda/concurrency_eval/bootstrap
```