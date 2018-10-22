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

(defn send-all
  [con-info messages]
    (write-to con-info messages))

(defn send-messages
  [con-info messages]
  (send-all con-info messages))
