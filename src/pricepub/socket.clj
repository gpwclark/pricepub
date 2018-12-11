(ns pricepub.socket
  (:import (java.net Socket InetSocketAddress)
           (java.nio ByteBuffer)
           (java.nio.channels SocketChannel)))

(defn get-socket-addr
  [con-info]
  (let [{:keys [host port]} con-info]
    (InetSocketAddress. host port)))

(defn send-on-sock
  [socket-chan message]
  (let [write-buf (ByteBuffer/wrap (.getBytes message "UTF-8"))]
    (loop [write-buf write-buf]
      (when (.hasRemaining write-buf)
        (.write socket-chan write-buf)
        (Thread/sleep 10) ;;get this out of here?
        (recur write-buf)))
    (println (str "sent: " message))
    (.clear write-buf)))

(defn write-to
  [con-info messages]
  (let [socket-addr (get-socket-addr con-info)]
    (with-open [socket-chan (SocketChannel/open socket-addr)]
      (loop [messages messages]
       (when (not (nil? (first messages)))
         (send-on-sock socket-chan (first messages))
         (recur (rest messages)))))))

(defn read-from-sock
  [socket-chan]
  (let [read-buf (ByteBuffer/allocate 8192)]
    (loop [read-buf read-buf bytes-read 0]
      (if (<= bytes-read 0)
        (recur read-buf (.read socket-chan read-buf))
        (let [position (.position read-buf)
              dst-array (byte-array position)
              flip (.flip read-buf)
              res-bytes (.get read-buf dst-array 0 position)
              clear-buf (.clear read-buf)]
          (String. dst-array))))))

(defn write-to-once
  [con-info message]
  (let
      [socket-addr (get-socket-addr con-info)]
    (with-open [socket-chan (SocketChannel/open socket-addr)]
      (send-on-sock socket-chan message)
      (read-from-sock socket-chan))))

(defn write-to-once
  [con-info message]
  (let
      [socket-addr (get-socket-addr con-info)]
    (with-open [socket-chan (SocketChannel/open socket-addr)]
      (send-on-sock socket-chan message)
      (read-from-sock socket-chan))))
