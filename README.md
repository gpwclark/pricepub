# pricepub

A Clojure library designed to publish messages to a server
implementing the extra-curricular message passing assignment's publisher API.

## Usage
-   If you haven't already installed lein do so, you need it to make the
uberjar.
-   To make the runnable jar:
```
lein uberjar
```
-   To send a message on a topic to a server:
```
java -jar target/pricepub-0.1.0-SNAPSHOT-standalone.jar "TOPIC" "message"
```
-   This impl of the publisher API supports batching. Additional messages are 
separated by white space and are published at once (in order) to the server on
the provided topic.
```
java -jar target/pricepub-0.1.0-SNAPSHOT-standalone.jar "TOPIC" "message1" "message2" ...
```

## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
