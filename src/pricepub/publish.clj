(ns pricepub.publish
  (:require [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.data.json :as json]
            [clojure.java.io :refer [writer reader copy output-stream]])
  (:import (java.net Socket InetSocketAddress)
           (java.nio ByteBuffer)
           (java.nio.channels SocketChannel)))

(defn send-on-sock
  [socket-chan message]
  (let [write-buf (ByteBuffer/wrap (.getBytes message "UTF-8"))]
    (loop [write-buf write-buf]
      (when (.hasRemaining write-buf)
        (.write socket-chan write-buf)
        (Thread/sleep 10)
        (recur write-buf)))
    (println (str "sent: " message))
    (.clear write-buf)))

(defn write-to
  [con-info messages]
  (let [socket-addr (InetSocketAddress. (:host con-info) (:port con-info))]
    (with-open [socket-chan (SocketChannel/open socket-addr)]
      (loop [messages messages]
       (when (not (nil? (first messages)))
         (send-on-sock socket-chan (first messages))
         (recur (rest messages)))))))

(defn send-messages
  [con-info messages]
    (write-to con-info messages))

(defn verify-response
  [res]
  (-> (json/read-str res)
      (clojure.walk/keywordize-keys)
      (:status)
      (= "OK")))

(defn write-to-once
  [con-info message]
  (let
    [socket-addr (InetSocketAddress. (:host con-info) (:port con-info))]
    (with-open [socket-chan (SocketChannel/open socket-addr)]
      (let
          [write-buf (ByteBuffer/wrap (.getBytes message "UTF-8"))
            read-buf (ByteBuffer/allocate 1024)]
        (send-on-sock socket-chan message)
        (loop [read-buf read-buf bytes-read 0]
          (if (<= bytes-read 0)
            (recur read-buf (.read socket-chan read-buf))
            (let [position (.position read-buf)
                  dst-array (byte-array position)
                  flip (.flip read-buf)
                  res-bytes (.get read-buf dst-array 0 position)
                  clear-buf (.clear read-buf)]
              (String. dst-array))))))))

(defn send-message
  [con-info message]
  (let [res (write-to-once con-info message)
        success (verify-response res)]
    (if success
      (println "send message succeeded.")
      (println "send message failed."))))
