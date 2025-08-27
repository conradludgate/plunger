# Plunger quickly unblocks your async tasks.

This crate is designed to help you run CPU intensive code in your async applications.
It does so with 0 allocations per task, and intends to allow in-place initialisation of responses.

## Example

```rust
#[tokio::main]
async fn main() {
    let hash = "$argon2i$v=19$m=65536,t=1,p=1$c29tZXNhbHQAAAAAAAAAAA$+r0d29hqEB0yasKr55ZgICsQGSkl0v0kgwhd+U3wyRo";
    let password = "password";

    plunger::unblock(move || password_auth::verify_password(password, hash))
        .await
        .unwrap();
}
```

## Important notes

While the intent is to unblock the async runtime, this API might have to defensively block the runtime if
cancellation occurs while the task is running.

We assume the following:
1. Cancellation is rare
2. Tasks run in the range of 100us to 1ms
3. We can use block_in_place to reduce the impact of blocking when the tokio feature is enabled and using a multithreaded runtime.
