(ns pricepub.publish
  (:require [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.data.json :as json]
            [clojure.java.io :refer [writer reader copy output-stream]])
  (:import (java.net Socket InetSocketAddress)
           (java.nio ByteBuffer)
           (java.nio.channels SocketChannel)))

(defn write-to
  [con-info messages]
  (let [socket-addr (InetSocketAddress. (:host con-info) (:port con-info))]
    (with-open [socket-chan (SocketChannel/open socket-addr)]
      (loop [messages messages]
       (when (not (nil? (first messages)))
         (let
             [buf (ByteBuffer/wrap (.getBytes (first messages) "UTF-8"))]
           (loop [buf buf]
             (when (.hasRemaining buf)
                 (.write socket-chan buf)
                 (Thread/sleep 10)
                 (recur buf)))
           (println "sent: " (str (first messages)))
           (.clear buf))
         (recur (rest messages)))))))

(defn send-messages
  [con-info messages]
    (write-to con-info messages))

(defn verify-response
  [res]
  (= "OK" (:status (clojure.walk/keywordize-keys (json/read-str res)))))

(defn write-to-once
  [con-info message]
  (let
    [socket-addr (InetSocketAddress. (:host con-info) (:port con-info))]
    (with-open [socket-chan (SocketChannel/open socket-addr)]
      (let
          [write-buf (ByteBuffer/wrap (.getBytes message "UTF-8"))
            read-buf (ByteBuffer/allocate 1024)]
        (loop [write-buf write-buf]
          (when (.hasRemaining write-buf)
            (.write socket-chan write-buf)
            (Thread/sleep 10)
            (recur write-buf)))
        (println (str "sent: " message))
        (.clear write-buf)
        (loop [read-buf read-buf bytes-read 0]
          (if (<= bytes-read 0)
            (recur read-buf (.read socket-chan read-buf))
            (let [position (.position read-buf)
                  dst-array (byte-array position)
                  flip (.flip read-buf)
                  res-bytes (.get read-buf dst-array 0 position)]
              (String. dst-array))))
        (.clear read-buf)))))

(defn send-message
  [con-info message]
  (let [res (write-to-once con-info message)
        success (verify-response res)]
    (if success
      (println "send message succeeded.")
      (println "send message failed."))))
