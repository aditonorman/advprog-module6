## Commit 1 Reflection notes
In this commit, I dove into building a simple, single-threaded web server in Rust. I set up a TcpListener on 127.0.0.1:7878 to accept connections from my browser, and used a BufReader to process each HTTP request line by line until I reached the end of the header section. This hands-on experience not only gave me a clearer picture of how HTTP requests work but also highlighted Rust’s powerful network programming capabilities. Even though I used unwrap() for simplicity, it was a great reminder of the importance of proper error handling, which I plan to refine later. Overall, this project has been both challenging and rewarding, and I'm excited to explore further improvements like better response handling and concurrency in the future.