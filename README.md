## Commit 1 Reflection notes
In this commit, I dove into building a simple, single-threaded web server in Rust. I set up a TcpListener on 127.0.0.1:7878 to accept connections from my browser, and used a BufReader to process each HTTP request line by line until I reached the end of the header section. This hands-on experience not only gave me a clearer picture of how HTTP requests work but also highlighted Rust’s powerful network programming capabilities. Even though I used unwrap() for simplicity, it was a great reminder of the importance of proper error handling, which I plan to refine later. Overall, this project has been both challenging and rewarding, and I'm excited to explore further improvements like better response handling and concurrency in the future.

## Commit 2 Reflection notes
![Commit 2 screen capture](/assets/images/commit2.png)

In this commit, I took a significant step forward by updating my Rust web server to serve a proper HTML page. I learned that returning a web page involves more than just sending raw data; it requires crafting a well-formed HTTP response that includes essential headers like "Content-Length" so that the browser knows how much data to expect. Reading the HTML file from disk, calculating its length, and formatting the response correctly with CRLF line breaks deepened my understanding of HTTP protocols and the inner workings of a web server. This hands-on experience has not only boosted my confidence in handling file I/O and string manipulation in Rust, but also highlighted the importance of following protocol standards to ensure seamless communication between the server and the browser.

## Commit 3 Reflection notes
![Commit 3 screen capture](/assets/images/commit3.png)

In this commit, I refactored my Rust web server to validate incoming HTTP requests and selectively respond based on the requested URL. Instead of serving the same content regardless of the URL, I now inspect the first line of the request. If it matches "GET / HTTP/1.1", the server responds with a 200 OK status and serves the hello.html file. For any other request, it returns a 404 NOT FOUND status along with a custom 404.html page. This approach not only simulates basic routing—similar to what production servers perform—but also improves code organization by separating the logic for handling valid and invalid requests. Through this process, I deepened my understanding of HTTP status codes, proper header formatting, and the importance of maintainable code in server design.

